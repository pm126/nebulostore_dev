package org.nebulostore.replicaresolver;

import java.io.IOException;

/**
 * Factory for producing ContractMaps.
 *
 * @author Grzegorz Milka
 */
public interface ReplicaResolverFactory {
  ReplicaResolver getContractMap();
  void startUp() throws IOException;
  void shutDown();
}
