package org.nebulostore.appcore.model;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;
import org.nebulostore.appcore.addressing.NebuloAddress;
import org.nebulostore.appcore.addressing.ObjectId;
import org.nebulostore.appcore.exceptions.NebuloException;
import org.nebulostore.replicator.core.TransactionAnswer;
import org.nebulostore.subscription.model.SubscriptionNotification.NotificationReason;

/**
 * File. Only metadata is stored in this object, real data is represented by FileChunk.
 * Constructors are package-protected. Use NebuloObjectFactoryImpl to create objects.
 * @author Bolek Kulbabinski
 */
public class NebuloFile extends NebuloObject {

  /**
   * File chunk metadata.
   */
  protected class FileChunkWrapper implements Serializable {
    private static final long serialVersionUID = -6723808968821818811L;

    // FileChunk stores (endByte_ - startByte_) bytes from interval [startByte_, endByte_).
    private final int startByte_;
    private int endByte_;
    private final NebuloAddress chunkAddress_;

    // These fields are set to null after deserializaton of NebuloFile.
    private transient FileChunk chunk_;
    private transient boolean isChanged_;

    public FileChunkWrapper(int startByte, int endByte, NebuloAddress address) {
      startByte_ = startByte;
      endByte_ = endByte;
      chunkAddress_ = address;
      chunk_ = new FileChunk(address, endByte_ - startByte_);
      isChanged_ = true;
    }

    public byte[] getData() throws NebuloException {
      if (chunk_ == null) {
        // Use metadata holder for chunk query.
        // TODO(bolek): Retry with full address if unsuccessful.
        ObjectGetter getter = objectGetterProvider_.get();
        getter.fetchObject(chunkAddress_);
        chunk_ = (FileChunk) getter.awaitResult(TIMEOUT_SEC);
      }
      return chunk_.getData();
    }

    public void setData(byte[] data) {
      chunk_.setData(data);
      isChanged_ = true;
    }

    public ObjectDeleter deleteChunk() throws NebuloException {
      ObjectDeleter deleter = objectDeleterProvider_.get();
      deleter.deleteObject(chunkAddress_);
      return deleter;
    }

    public ObjectWriter sync() {
      if (isChanged_) {
        ObjectWriter writer = objectWriterProvider_.get();
        writer.writeObject(chunk_, chunk_.getVersions());
        return writer;
      } else {
        return null;
      }
    }

    public void setSynced() {
      isChanged_ = false;
    }

    public boolean isChanged() {
      return isChanged_;
    }

    public FileChunk getFileChunk() {
      return chunk_;
    }

    private void writeObject(ObjectOutputStream out) throws IOException {
      out.defaultWriteObject();
    }

    private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
      in.defaultReadObject();
      chunk_ = null;
      isChanged_ = false;
    }
  }

  private static Logger logger_ = Logger.getLogger(NebuloFile.class);
  private static final long serialVersionUID = -1687075358113579488L;
  public static final int DEFAULT_CHUNK_SIZE_BYTES = 1024 * 1024;

  // Is this file new, i.e. its ObjectId is not yet in DHT (false by default).
  protected transient boolean isNew_;

  protected int size_;
  protected List<FileChunkWrapper> chunks_;
  protected int chunkSize_ = DEFAULT_CHUNK_SIZE_BYTES;

  public NebuloFile(NebuloAddress address) {
    this(address, DEFAULT_CHUNK_SIZE_BYTES);
  }

  public NebuloFile(NebuloAddress address, int chunkSize) {
    super(address);
    chunkSize_ = chunkSize;
    initNewFile();
  }

  public int getSize() {
    return size_;
  }

  /**
   * Read at most len bytes starting from position pos.
   * @param pos
   * @param len
   * @return bytes read
   * @throws NebuloException
   */
  public byte[] read(int pos, int len) throws NebuloException {
    logger_.info("read called");

    int trueLen = Math.min(len, size_ - pos);
    int truePos = pos;
    byte[] ret = new byte[trueLen];
    int retPos = 0;

    FileChunkWrapper chunk = null;
    for (int i = 0; i < chunks_.size() && trueLen > 0; ++i) {
      logger_.debug("Fetching chunk... " + i);
      chunk = chunks_.get(i);
      if (chunk.startByte_ <= truePos && chunk.endByte_ > truePos) {
        int currLen = Math.min(trueLen, chunk.endByte_ - truePos);
        try {
          System.arraycopy(chunk.getData(), truePos - chunk.startByte_, ret, retPos, currLen);
        } catch (NebuloException exception) {
          throw new NebuloException("Unable to fetch file", exception);
        }
        trueLen -= currLen;
        truePos += currLen;
        retPos += currLen;
      }
    }
    return ret;
  }

 /**
  * Write bytes from buffer starting at position pos.
  * @param buffer
  * @param pos
  * @return bytes written
  * @throws NebuloException
  */
  public int write(byte[] buffer, int pos) throws NebuloException {
    if (pos > size_) {
      throw new NebuloException("Write attempt after the end of the file!");
    }
    int chunkIndex = pos / chunkSize_;
    int currPos = pos;
    int currBufPos = 0;
    while (currPos < pos + buffer.length) {
      int currChunkPos = currPos - chunkIndex * chunkSize_;
      int currLen = Math.min(chunkSize_ - currChunkPos, buffer.length + pos - currPos);
      writeSingleChunk(chunkIndex, buffer, currBufPos, currChunkPos, currLen);
      currBufPos += currLen;
      currPos += currLen;
      ++chunkIndex;
    }
    size_ = Math.max(size_, pos + buffer.length);
    logger_.debug("New size: " + size_);
    runSync();
    return buffer.length;
  }

  private void writeSingleChunk(int chunkIdx, byte[] buffer, int bufPos, int chunkPos, int len)
      throws NebuloException {
    if (chunkIdx >= chunks_.size()) {
      // Create new chunk at the end.
      // TODO(bolek): Better ID generation!
      ObjectId chunkId = new ObjectId(
          chunks_.get(chunks_.size() - 1).chunkAddress_.getObjectId().getKey().add(BigInteger.ONE));
      chunks_.add(new FileChunkWrapper(chunkIdx * chunkSize_, chunkIdx * chunkSize_ + len,
          new NebuloAddress(address_.getAppKey(), chunkId)));
    }
    FileChunkWrapper chunk = chunks_.get(chunkIdx);
    try {
      byte[] newDataArray = chunk.getData();
      int endPos = chunkIdx * chunkSize_ + (chunkPos + len);
      if (endPos > chunk.endByte_) {
        // Resize chunk.
        newDataArray = new byte[endPos - chunk.startByte_];
        System.arraycopy(chunk.getData(), 0, newDataArray, 0, chunk.endByte_ - chunk.startByte_);
        chunk.endByte_ = endPos;
      }
      System.arraycopy(buffer, bufPos, newDataArray, chunkPos, len);
      chunk.setData(newDataArray);
    } catch (NebuloException exception) {
      throw new NebuloException("Unable to fetch file", exception);
    }
  }

  /**
   * Truncates the file to newSize.
   * @param newSize
   * @throws NebuloException The exception is thrown when newSize is greater than current size.
   */
  public void truncate(int newSize) throws NebuloException {
    if (newSize > size_) {
      throw new NebuloException("Cannot truncate file to greater size!");
    }
    int nChunks = (newSize + (chunkSize_ - 1)) / chunkSize_;
    /// Remove unneeded chunks.
    while (chunks_.size() > nChunks) {
      chunks_.get(chunks_.size() - 1).deleteChunk();
      chunks_.remove(chunks_.size() - 1);
    }
    // Truncate last chunk if necessary.
    FileChunkWrapper chunk = chunks_.get(chunks_.size() - 1);
    if (chunk.endByte_ > newSize) {
      byte[] newData = new byte[newSize - chunk.startByte_];
      System.arraycopy(chunk.getData(), 0, newData, 0, newSize - chunk.startByte_);
      chunk.setData(newData);
      chunk.endByte_ = newSize;
    }
    // Set file size.
    size_ = newSize;
  }

  private void initNewFile() {
    isNew_ = true;
    chunks_ = new ArrayList<FileChunkWrapper>();
    // TODO(bolek): How to assign IDs for new chunks?
    ObjectId chunkId = new ObjectId(address_.getObjectId().getKey().add(BigInteger.ONE));
    chunks_.add(new FileChunkWrapper(0, 0, new NebuloAddress(address_.getAppKey(), chunkId)));
  }

  @Override
  protected void runSync() throws NebuloException {
    logger_.info("Running sync on file ");
    List<ObjectWriter> updateModules = new ArrayList<ObjectWriter>();
    // Run sync for all chunks in parallel.
    for (int i = 0; i < chunks_.size(); ++i) {
      logger_.debug("Creating update module for chunk " + i);
      updateModules.add(chunks_.get(i).sync());
    }
    // Run sync for NebuloFile (metadata).
    ObjectWriter writer = objectWriterProvider_.get();
    writer.writeObject(this, previousVersions_);
    updateModules.add(writer);

    // Wait for all results.
    NebuloException caughtException = null;
    for (int i = 0; i < updateModules.size(); ++i) {
      try {
        if (updateModules.get(i) != null) {
          logger_.debug("Waiting for next module: " + updateModules.get(i));
          updateModules.get(i).getSemiResult(TIMEOUT_SEC);
        }
      } catch (NebuloException exception) {
        caughtException = exception;
      }
    }

    logger_.debug("Caught exception: " + caughtException);
    if (caughtException != null) {
      // aborting the transaction
      logger_.debug("Aborting, update modules: " + updateModules);
      for (ObjectWriter writerModule : updateModules) {
        if (writerModule != null) {
          writerModule.setAnswer(TransactionAnswer.ABORT);
        }
      }
      throw caughtException;
    } else {
      logger_.debug("Commiting transaction, update modules: " + updateModules);
      for (ObjectWriter writerModule : updateModules) {
        if (writerModule != null) {
          writerModule.setAnswer(TransactionAnswer.COMMIT);
        }
      }
      for (FileChunkWrapper chunk : chunks_) {
        chunk.setSynced();
      }
      notifySubscribers(NotificationReason.FILE_CHANGED);
    }
    for (ObjectWriter writerModule : updateModules) {
      if (writerModule != null) {
        writerModule.awaitResult(TIMEOUT_SEC);
      }
    }
  }

  @Override
  public void delete() throws NebuloException {
    logger_.info("Running delete on file.");
    List<ObjectDeleter> deleteModules = new ArrayList<ObjectDeleter>();
    // Run delete for all chunks in parallel.
    for (int i = 0; i < chunks_.size(); ++i) {
      logger_.debug("Creating delete module for chunk " + i);
      deleteModules.add(chunks_.get(i).deleteChunk());
    }
    // Run delete for NebuloFile (metadata).
    ObjectDeleter deleter = objectDeleterProvider_.get();
    deleter.deleteObject(address_);
    deleteModules.add(deleter);
    notifySubscribers(NotificationReason.FILE_DELETED);
    // Wait for all results.
    for (int i = 0; i < deleteModules.size(); ++i) {
      if (deleteModules.get(i) != null) {
        deleteModules.get(i).awaitResult(TIMEOUT_SEC);
      }
    }
  }

  private void writeObject(ObjectOutputStream out) throws IOException {
    out.defaultWriteObject();
  }

  private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
    in.defaultReadObject();
    isNew_ = false;
  }
}

