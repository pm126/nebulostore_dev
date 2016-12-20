package org.nebulostore.replicator.core;

import java.util.Collections;
import java.util.Map;
import java.util.Set;

import org.nebulostore.appcore.modules.JobModule;

/**
 * Base class for all replicators.
 *
 * @author Bolek Kulbabinski
 */
public abstract class Replicator extends JobModule {

  public static class MetaData {
    private final String objectId_;
    private final Integer wholeObjectSize_;

    public MetaData(String objectId, Integer size) {
      this.objectId_ = objectId;
      this.wholeObjectSize_ = size;
    }

    public String getObjectId() {
      return objectId_;
    }

    public Integer getWholeObjectSize() {
      return wholeObjectSize_;
    }
  }

  protected Map<String, MetaData> storedObjectsMeta_;

  public Set<String> getStoredObjectsIds() {
    return Collections.unmodifiableSet(storedObjectsMeta_.keySet());
  }

  public Map<String, MetaData> getStoredMetaData() {
    return Collections.unmodifiableMap(storedObjectsMeta_);
  }

  public abstract void cacheStoredObjectsMeta();
}
