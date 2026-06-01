package io.envoyproxy.controlplane.server;

import static io.envoyproxy.controlplane.cache.Resources.V3.ROUTE_TYPE_URL;
import static org.assertj.core.api.Assertions.assertThat;

import com.google.common.collect.ImmutableMap;
import java.util.Collections;
import org.junit.Test;

/**
 * Regression tests for the delta subscription bookkeeping. When the cache REMOVES a resource (it no longer
 * exists), that is not the same as the client UNSUBSCRIBING from it: the client is still subscribed, so if the
 * resource is later (re)created it must be delivered. The observer must therefore keep a cache-removed resource
 * subscribed (pending) rather than forgetting it entirely.
 *
 * <p>This reproduces the delete -> re-add desync observed end-to-end against a real Envoy on a delta ADS stream.
 */
public class DeltaSubscriptionBookkeepingTest {

  @Test
  public void xdsCacheRemovalKeepsResourceSubscribed() {
    XdsDeltaDiscoveryRequestStreamObserver<Object, Object, Object> observer =
        new XdsDeltaDiscoveryRequestStreamObserver<>(ROUTE_TYPE_URL, null, 0L, null, null);

    observer.updateSubscriptions(ROUTE_TYPE_URL, Collections.singletonList("r0"), Collections.emptyList());
    observer.updateTrackedResources(ROUTE_TYPE_URL, ImmutableMap.of("r0", "v1"), Collections.emptyList());
    assertThat(observer.resourceVersions(ROUTE_TYPE_URL)).containsKey("r0");
    assertThat(observer.pendingResources(ROUTE_TYPE_URL)).doesNotContain("r0");

    // Cache removes r0 (it no longer exists). The client did NOT unsubscribe.
    observer.updateTrackedResources(ROUTE_TYPE_URL, Collections.emptyMap(), Collections.singletonList("r0"));

    assertThat(observer.resourceVersions(ROUTE_TYPE_URL)).doesNotContainKey("r0");
    // r0 must remain subscribed so a later re-create is delivered to the still-subscribed client.
    assertThat(observer.pendingResources(ROUTE_TYPE_URL)).contains("r0");
  }

  @Test
  public void adsCacheRemovalKeepsResourceSubscribed() {
    AdsDeltaDiscoveryRequestStreamObserver<Object, Object, Object> observer =
        new AdsDeltaDiscoveryRequestStreamObserver<>(null, 0L, null, null);

    observer.updateSubscriptions(ROUTE_TYPE_URL, Collections.singletonList("r0"), Collections.emptyList());
    observer.updateTrackedResources(ROUTE_TYPE_URL, ImmutableMap.of("r0", "v1"), Collections.emptyList());
    assertThat(observer.resourceVersions(ROUTE_TYPE_URL)).containsKey("r0");

    observer.updateTrackedResources(ROUTE_TYPE_URL, Collections.emptyMap(), Collections.singletonList("r0"));

    assertThat(observer.resourceVersions(ROUTE_TYPE_URL)).doesNotContainKey("r0");
    assertThat(observer.pendingResources(ROUTE_TYPE_URL)).contains("r0");
  }

  @Test
  public void explicitUnsubscribeDropsResourceEntirely() {
    // Contrast: a real unsubscribe must NOT keep the resource pending.
    XdsDeltaDiscoveryRequestStreamObserver<Object, Object, Object> observer =
        new XdsDeltaDiscoveryRequestStreamObserver<>(ROUTE_TYPE_URL, null, 0L, null, null);

    observer.updateSubscriptions(ROUTE_TYPE_URL, Collections.singletonList("r0"), Collections.emptyList());
    observer.updateTrackedResources(ROUTE_TYPE_URL, ImmutableMap.of("r0", "v1"), Collections.emptyList());

    observer.updateSubscriptions(ROUTE_TYPE_URL, Collections.emptyList(), Collections.singletonList("r0"));

    assertThat(observer.resourceVersions(ROUTE_TYPE_URL)).doesNotContainKey("r0");
    assertThat(observer.pendingResources(ROUTE_TYPE_URL)).doesNotContain("r0");
  }
}
