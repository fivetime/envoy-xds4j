package io.envoyproxy.controlplane.server;

import com.google.common.collect.ImmutableList;
import com.google.protobuf.Any;
import com.google.protobuf.Duration;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.UInt32Value;
import com.sun.net.httpserver.HttpServer;
import io.envoyproxy.controlplane.cache.Cache;
import io.envoyproxy.controlplane.cache.LinearCache;
import io.envoyproxy.controlplane.cache.MuxCache;
import io.envoyproxy.controlplane.cache.Resources.ResourceType;
import io.envoyproxy.controlplane.cache.TestResources;
import io.envoyproxy.controlplane.cache.v3.SimpleCache;
import io.envoyproxy.controlplane.cache.v3.Snapshot;
import io.envoyproxy.envoy.config.cluster.v3.Cluster;
import io.envoyproxy.envoy.config.core.v3.AggregatedConfigSource;
import io.envoyproxy.envoy.config.core.v3.ApiVersion;
import io.envoyproxy.envoy.config.core.v3.ConfigSource;
import io.envoyproxy.envoy.config.core.v3.DataSource;
import io.envoyproxy.envoy.config.core.v3.HeaderValue;
import io.envoyproxy.envoy.config.core.v3.HeaderValueOption;
import io.envoyproxy.envoy.config.core.v3.RuntimeFractionalPercent;
import io.envoyproxy.envoy.config.core.v3.TransportSocket;
import io.envoyproxy.envoy.config.listener.v3.Filter;
import io.envoyproxy.envoy.config.listener.v3.Listener;
import io.envoyproxy.envoy.config.route.v3.RouteConfiguration;
import io.envoyproxy.envoy.config.route.v3.VirtualHost;
import io.envoyproxy.envoy.extensions.filters.http.local_ratelimit.v3.LocalRateLimit;
import io.envoyproxy.envoy.extensions.filters.network.http_connection_manager.v3.HttpConnectionManager;
import io.envoyproxy.envoy.extensions.filters.network.http_connection_manager.v3.HttpFilter;
import io.envoyproxy.envoy.extensions.transport_sockets.tls.v3.CommonTlsContext;
import io.envoyproxy.envoy.extensions.transport_sockets.tls.v3.DownstreamTlsContext;
import io.envoyproxy.envoy.extensions.transport_sockets.tls.v3.SdsSecretConfig;
import io.envoyproxy.envoy.extensions.transport_sockets.tls.v3.Secret;
import io.envoyproxy.envoy.extensions.transport_sockets.tls.v3.TlsCertificate;
import io.envoyproxy.envoy.type.v3.FractionalPercent;
import io.envoyproxy.envoy.type.v3.TokenBucket;
import io.grpc.Server;
import io.grpc.netty.NettyServerBuilder;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.EnumMap;
import java.util.Map;

/**
 * Minimal standalone xDS control plane for end-to-end / Kubernetes testing of the fork's caches.
 *
 * <p>Wiring (mirrors the design doc layout): a {@link MuxCache} routes RDS to a {@link LinearCache} (so routes
 * can be updated at O(1) at runtime) and CDS/LDS/EDS/SDS to a {@link SimpleCache}. It serves:
 * <ul>
 *   <li>LDS: a listener bound to 0.0.0.0:{@code LISTENER_PORT} whose HCM fetches RDS {@code route0} via ADS;</li>
 *   <li>RDS: {@code route0} routing everything to cluster {@code upstream} (served from the LinearCache);</li>
 *   <li>CDS: {@code upstream} -> {@code UPSTREAM_HOST}:{@code UPSTREAM_PORT} (STRICT_DNS).</li>
 * </ul>
 *
 * <p>Configuration via environment variables (with defaults):
 * {@code XDS_PORT} (18000), {@code LISTENER_PORT} (10000), {@code UPSTREAM_HOST} (127.0.0.1),
 * {@code UPSTREAM_PORT} (8080). The gRPC server binds 0.0.0.0 so it is reachable over host networking.
 */
public final class ControlPlaneMain {

  private static final String GROUP = "key";
  private static final String SECRET_NAME = "cert:test.local";

  // Runtime route state for route0: a catch-all default vhost plus zero or more per-domain vhosts.
  private static final Map<String, String> domains = new java.util.concurrent.ConcurrentHashMap<>();
  private static volatile String defaultHeader = null;
  // Local rate limit applied to the default vhost: N requests / 60s, enforced (0 = off).
  private static volatile int rateLimitRps = 0;

  private static int envInt(String name, int dflt) {
    String v = System.getenv(name);
    return v == null || v.isEmpty() ? dflt : Integer.parseInt(v);
  }

  private static String envStr(String name, String dflt) {
    String v = System.getenv(name);
    return v == null || v.isEmpty() ? dflt : v;
  }

  /**
   * Starts the control plane and blocks.
   *
   * @param args unused
   * @throws Exception on startup or await failure
   */
  public static void main(String[] args) throws Exception {
    final int xdsPort = envInt("XDS_PORT", 18000);
    final int listenerPort = envInt("LISTENER_PORT", 10000);
    final String upstreamHost = envStr("UPSTREAM_HOST", "127.0.0.1");
    final int upstreamPort = envInt("UPSTREAM_PORT", 8080);

    final int tlsPort = envInt("TLS_PORT", 10443);
    final String certDir = envStr("CERT_DIR", "/certs");

    // SDS on LinearCache -> certificate rotation is an O(1) updateResource on the same secret name.
    LinearCache<String> secretCache = new LinearCache<>(ResourceType.SECRET, GROUP);
    secretCache.updateResource(SECRET_NAME, buildSecret(certDir, 1));

    // CDS on LinearCache -> switching/adding a backend is an O(1) updateResource (STRICT_DNS embeds endpoints).
    LinearCache<String> clusterCache = new LinearCache<>(ResourceType.CLUSTER, GROUP);
    clusterCache.updateResource("upstream", TestResources.createCluster("upstream", upstreamHost, upstreamPort,
        Cluster.DiscoveryType.STRICT_DNS));

    // LDS/EDS on SimpleCache (static side). Two listeners: plain HTTP + TLS (cert served via SDS).
    SimpleCache<String> simpleCache = new SimpleCache<>(node -> GROUP);
    Listener listener = withLocalRateLimitFilter(TestResources.createListener(true, true, ApiVersion.V3,
        ApiVersion.V3, "listener0", listenerPort, "route0"));
    Listener tlsListener = buildTlsListener("listener-tls", tlsPort, "route0", SECRET_NAME);
    simpleCache.setSnapshot(GROUP, Snapshot.create(
        ImmutableList.of(),
        ImmutableList.of(),
        ImmutableList.of(listener, tlsListener),
        ImmutableList.of(),
        ImmutableList.of(),
        "1"));

    // RDS on LinearCache (high-churn / O(1) side) -- this is the fork's new code under test.
    LinearCache<String> routeCache = new LinearCache<>(ResourceType.ROUTE, GROUP);
    routeCache.updateResource("route0", currentRoute0());

    Map<ResourceType, Cache<String>> caches = new EnumMap<>(ResourceType.class);
    caches.put(ResourceType.CLUSTER, clusterCache);
    caches.put(ResourceType.LISTENER, simpleCache);
    caches.put(ResourceType.ENDPOINT, simpleCache);
    caches.put(ResourceType.SECRET, secretCache);
    caches.put(ResourceType.ROUTE, routeCache);
    MuxCache<String> mux = new MuxCache<>(caches);

    V3DiscoveryServer discoveryServer = new V3DiscoveryServer(mux);
    Server server = NettyServerBuilder.forPort(xdsPort)
        .addService(discoveryServer.getAggregatedDiscoveryServiceImpl())
        .addService(discoveryServer.getClusterDiscoveryServiceImpl())
        .addService(discoveryServer.getEndpointDiscoveryServiceImpl())
        .addService(discoveryServer.getListenerDiscoveryServiceImpl())
        .addService(discoveryServer.getRouteDiscoveryServiceImpl())
        .addService(discoveryServer.getSecretDiscoveryServiceImpl())
        .build();

    server.start();

    // Tiny runtime-control HTTP endpoint to demo LinearCache.updateResource hot-applying without reconnect.
    //   GET /update?h=<value>  -> route0 gets a response header x-xds4j-route=<value> (empty -> plain route)
    int controlPort = envInt("CONTROL_PORT", 18001);
    HttpServer http = HttpServer.create(new InetSocketAddress(controlPort), 0);
    http.createContext("/update", exchange -> {
      String query = exchange.getRequestURI().getQuery();
      String header = null;
      if (query != null) {
        for (String kv : query.split("&")) {
          if (kv.startsWith("h=")) {
            header = kv.substring(2);
          }
        }
      }
      // O(1) runtime update of a single resource in the LinearCache; pushed to Envoy over the live ADS stream.
      defaultHeader = header;
      routeCache.updateResource("route0", currentRoute0());
      byte[] body = ("updated route0 (x-xds4j-route=" + (header == null || header.isEmpty() ? "<none>" : header)
          + ")\n").getBytes(StandardCharsets.UTF_8);
      exchange.sendResponseHeaders(200, body.length);
      exchange.getResponseBody().write(body);
      exchange.close();
    });
    http.createContext("/delete", exchange -> {
      // O(1) runtime removal of route0 from the LinearCache; the removal is pushed to Envoy over delta ADS.
      routeCache.deleteResource("route0");
      byte[] body = ("deleted route0 (cache now has " + routeCache.numResources() + " routes)\n")
          .getBytes(StandardCharsets.UTF_8);
      exchange.sendResponseHeaders(200, body.length);
      exchange.getResponseBody().write(body);
      exchange.close();
    });
    http.createContext("/empty", exchange -> {
      // updateResource route0 to an empty RouteConfiguration (no virtual hosts): a request then matches no
      // route, so Envoy returns 404. This is how a "domain removal" actually yields 404 when one route config
      // holds many virtual hosts (drop the vhost), vs. deleting the whole route config the listener references.
      routeCache.updateResource("route0", RouteConfiguration.newBuilder().setName("route0").build());
      byte[] body = "route0 replaced with empty route config (no virtual hosts)\n"
          .getBytes(StandardCharsets.UTF_8);
      exchange.sendResponseHeaders(200, body.length);
      exchange.getResponseBody().write(body);
      exchange.close();
    });
    http.createContext("/rotate", exchange -> {
      String query = exchange.getRequestURI().getQuery();
      int version = 1;
      if (query != null) {
        for (String kv : query.split("&")) {
          if (kv.startsWith("v=")) {
            version = Integer.parseInt(kv.substring(2));
          }
        }
      }
      // O(1) certificate rotation: same secret name, NEW content -> Envoy atomically swaps the certificate on
      // the TransportSocket; existing connections keep the old cert, new handshakes use the new one. No reconnect.
      secretCache.updateResource(SECRET_NAME, buildSecret(certDir, version));
      byte[] body = ("rotated " + SECRET_NAME + " to cert v" + version + "\n").getBytes(StandardCharsets.UTF_8);
      exchange.sendResponseHeaders(200, body.length);
      exchange.getResponseBody().write(body);
      exchange.close();
    });
    http.createContext("/add-domain", exchange -> {
      // Add a per-domain virtual host to route0 (O(1) updateResource). Requests with that Host match it.
      String host = queryParam(exchange.getRequestURI().getQuery(), "host");
      domains.put(host, host);
      routeCache.updateResource("route0", currentRoute0());
      respond(exchange, "added domain " + host + " (route0 now has " + (domains.size() + 1) + " vhosts)\n");
    });
    http.createContext("/remove-domain", exchange -> {
      // Remove a per-domain virtual host from route0 (O(1) updateResource). That Host falls back to the default.
      String host = queryParam(exchange.getRequestURI().getQuery(), "host");
      domains.remove(host);
      routeCache.updateResource("route0", currentRoute0());
      respond(exchange, "removed domain " + host + "\n");
    });
    http.createContext("/switch-backend", exchange -> {
      // O(1) backend switch: updateResource the 'upstream' cluster to point at a new host:port (STRICT_DNS).
      String query = exchange.getRequestURI().getQuery();
      String host = queryParam(query, "host");
      if (host.isEmpty()) {
        host = upstreamHost;
      }
      int port = Integer.parseInt(queryParam(query, "port"));
      clusterCache.updateResource("upstream",
          TestResources.createCluster("upstream", host, port, Cluster.DiscoveryType.STRICT_DNS));
      respond(exchange, "switched cluster upstream -> " + host + ":" + port + "\n");
    });
    http.createContext("/ratelimit", exchange -> {
      // Dynamically enable/adjust a per-vhost local rate limit via updateResource(route0). rps=0 disables it.
      rateLimitRps = Integer.parseInt(queryParam(exchange.getRequestURI().getQuery(), "rps"));
      routeCache.updateResource("route0", currentRoute0());
      respond(exchange, "local rate limit on default vhost = " + rateLimitRps + " req / 60s (0 = off)\n");
    });
    http.start();

    System.out.printf(
        "xDS control plane (MuxCache: RDS=LinearCache, CDS/LDS=SimpleCache) listening on 0.0.0.0:%d%n"
            + "  listener0 -> 0.0.0.0:%d  ->  route0  ->  cluster upstream -> %s:%d%n"
            + "  runtime control: GET http://<host>:%d/update?h=<value>  (LinearCache.updateResource route0)%n",
        xdsPort, listenerPort, upstreamHost, upstreamPort, controlPort);
    server.awaitTermination();
  }

  /**
   * Builds route0 from the current runtime state: one virtual host per entry in {@link #domains} (matching that
   * Host, tagged with an {@code x-domain} response header) followed by a catch-all default vhost (optionally
   * tagged with {@code x-xds4j-route}). All vhosts route to cluster {@code upstream}.
   */
  private static RouteConfiguration currentRoute0() {
    VirtualHost catchAll = TestResources.createRoute("route0", "upstream").getVirtualHosts(0);
    RouteConfiguration.Builder route = RouteConfiguration.newBuilder().setName("route0");
    for (Map.Entry<String, String> entry : domains.entrySet()) {
      route.addVirtualHosts(catchAll.toBuilder()
          .setName("vh-" + entry.getKey())
          .clearDomains()
          .addDomains(entry.getKey())
          .clearResponseHeadersToAdd()
          .addResponseHeadersToAdd(header("x-domain", entry.getValue()))
          .build());
    }
    VirtualHost.Builder defaultVhost = catchAll.toBuilder();
    if (defaultHeader != null && !defaultHeader.isEmpty()) {
      defaultVhost.addResponseHeadersToAdd(header("x-xds4j-route", defaultHeader));
    }
    if (rateLimitRps > 0) {
      defaultVhost.putTypedPerFilterConfig("envoy.filters.http.local_ratelimit",
          Any.pack(localRateLimit(rateLimitRps)));
    }
    return route.addVirtualHosts(defaultVhost.build()).build();
  }

  private static HeaderValueOption header(String key, String value) {
    return HeaderValueOption.newBuilder()
        .setHeader(HeaderValue.newBuilder().setKey(key).setValue(value).build())
        .build();
  }

  private static String queryParam(String query, String key) {
    if (query != null) {
      for (String kv : query.split("&")) {
        if (kv.startsWith(key + "=")) {
          return kv.substring(key.length() + 1);
        }
      }
    }
    return "";
  }

  private static void respond(com.sun.net.httpserver.HttpExchange exchange, String message) throws java.io.IOException {
    byte[] body = message.getBytes(StandardCharsets.UTF_8);
    exchange.sendResponseHeaders(200, body.length);
    exchange.getResponseBody().write(body);
    exchange.close();
  }

  /** Builds the SDS Secret for {@link #SECRET_NAME} from cert{version}.pem / key{version}.pem under certDir. */
  private static Secret buildSecret(String certDir, int version) throws java.io.IOException {
    String cert = new String(Files.readAllBytes(Paths.get(certDir, "cert" + version + ".pem")),
        StandardCharsets.UTF_8);
    String key = new String(Files.readAllBytes(Paths.get(certDir, "key" + version + ".pem")),
        StandardCharsets.UTF_8);
    return Secret.newBuilder()
        .setName(SECRET_NAME)
        .setTlsCertificate(TlsCertificate.newBuilder()
            .setCertificateChain(DataSource.newBuilder().setInlineString(cert))
            .setPrivateKey(DataSource.newBuilder().setInlineString(key)))
        .build();
  }

  /** Builds a TLS listener: same HCM as the plain listener, plus a TLS transport socket fetching the cert via SDS. */
  private static Listener buildTlsListener(String name, int port, String routeName, String secretName) {
    Listener plain = TestResources.createListener(true, true, ApiVersion.V3, ApiVersion.V3, name, port, routeName);
    DownstreamTlsContext tls = DownstreamTlsContext.newBuilder()
        .setCommonTlsContext(CommonTlsContext.newBuilder()
            .addTlsCertificateSdsSecretConfigs(SdsSecretConfig.newBuilder()
                .setName(secretName)
                .setSdsConfig(ConfigSource.newBuilder()
                    .setResourceApiVersion(ApiVersion.V3)
                    .setAds(AggregatedConfigSource.getDefaultInstance()))))
        .build();
    return plain.toBuilder()
        .setFilterChains(0, plain.getFilterChains(0).toBuilder()
            .setTransportSocket(TransportSocket.newBuilder()
                .setName("envoy.transport_sockets.tls")
                .setTypedConfig(Any.pack(tls)))
            .build())
        .build();
  }

  /** Inserts the local_ratelimit HTTP filter (disabled at the HCM level) so routes can enable it per-vhost. */
  private static Listener withLocalRateLimitFilter(Listener base) throws InvalidProtocolBufferException {
    Filter hcmFilter = base.getFilterChains(0).getFilters(0);
    HttpConnectionManager hcm = hcmFilter.getTypedConfig().unpack(HttpConnectionManager.class);
    HttpFilter rateLimit = HttpFilter.newBuilder()
        .setName("envoy.filters.http.local_ratelimit")
        .setTypedConfig(Any.pack(LocalRateLimit.newBuilder().setStatPrefix("http_local_rate_limit").build()))
        .build();
    HttpConnectionManager newHcm = hcm.toBuilder()
        .clearHttpFilters()
        .addHttpFilters(rateLimit)
        .addAllHttpFilters(hcm.getHttpFiltersList())
        .build();
    return base.toBuilder()
        .setFilterChains(0, base.getFilterChains(0).toBuilder()
            .setFilters(0, hcmFilter.toBuilder().setTypedConfig(Any.pack(newHcm)).build())
            .build())
        .build();
  }

  /** Per-vhost local rate limit: {@code rps} requests per 60s, fully enabled and enforced (HTTP 429 over limit). */
  private static LocalRateLimit localRateLimit(int rps) {
    RuntimeFractionalPercent hundred = RuntimeFractionalPercent.newBuilder()
        .setDefaultValue(FractionalPercent.newBuilder()
            .setNumerator(100)
            .setDenominator(FractionalPercent.DenominatorType.HUNDRED))
        .build();
    return LocalRateLimit.newBuilder()
        .setStatPrefix("http_local_rate_limit")
        .setTokenBucket(TokenBucket.newBuilder()
            .setMaxTokens(rps)
            .setTokensPerFill(UInt32Value.of(rps))
            .setFillInterval(Duration.newBuilder().setSeconds(60)))
        .setFilterEnabled(hundred)
        .setFilterEnforced(hundred)
        .build();
  }

  private ControlPlaneMain() {
  }
}
