package org.nebulostore.async.synchrogroup.selector;

import java.util.Set;

import com.google.common.collect.Sets;
import com.google.inject.Inject;
import com.google.inject.name.Named;

import org.nebulostore.async.AsyncMessagesContext;
import org.nebulostore.communication.naming.CommAddress;
import org.nebulostore.utils.Pair;

public class LimitedPeerNumSynchroPeerSelector implements SynchroPeerSelector {

  private static final String MAX_SYNCHRO_PEERS_NUMBER_PROPERTY_NAME =
      "async.limited-selector-limit";

  private final AsyncMessagesContext context_;
  private final CommAddress myAddress_;
  private final long maxSynchroPeersNumber_;

  @Inject
  public LimitedPeerNumSynchroPeerSelector(AsyncMessagesContext context, CommAddress myAddress,
      @Named(MAX_SYNCHRO_PEERS_NUMBER_PROPERTY_NAME) long maxSynchroPeersNumber) {
    context_ = context;
    myAddress_ = myAddress;
    maxSynchroPeersNumber_ = maxSynchroPeersNumber;
  }

  @Override
  public Pair<Set<CommAddress>, Set<CommAddress>> decide(CommAddress newPeer) {
    Set<CommAddress> peersToAdd = null;
    Set<CommAddress> synchroGroup = context_.getSynchroGroupForPeerCopy(myAddress_);
    if (synchroGroup.size() < maxSynchroPeersNumber_ && !synchroGroup.contains(newPeer)) {
      peersToAdd = Sets.newHashSet(newPeer);
    }
    return new Pair<Set<CommAddress>, Set<CommAddress>>(peersToAdd, null);
  }

}
