package org.nebulostore.replicator;

import java.io.IOException;
import java.math.BigInteger;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import com.google.common.base.Charsets;
import com.google.common.base.Preconditions;
import com.google.common.collect.Sets;

import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;
import org.nebulostore.appcore.addressing.ObjectId;
import org.nebulostore.appcore.messaging.Message;
import org.nebulostore.appcore.model.EncryptedObject;
import org.nebulostore.persistence.InMemoryStore;
import org.nebulostore.persistence.KeyValueStore;
import org.nebulostore.replicator.core.Replicator;
import org.nebulostore.replicator.core.Replicator.MetaData;
import org.nebulostore.replicator.core.TransactionAnswer;
import org.nebulostore.replicator.messages.ConfirmationMessage;
import org.nebulostore.replicator.messages.DeleteObjectMessage;
import org.nebulostore.replicator.messages.GetObjectsMessage;
import org.nebulostore.replicator.messages.QueryToStoreObjectsMessage;
import org.nebulostore.replicator.messages.ReplicatorErrorMessage;
import org.nebulostore.replicator.messages.SendObjectsMessage;
import org.nebulostore.replicator.messages.TransactionResultMessage;

/**
 * @author Bolek Kulbabinski
 */
public class ReplicatorTest {
  private static final ObjectId ID_1 = new ObjectId(new BigInteger("111"));
  private static final ObjectId ID_2 = new ObjectId(new BigInteger("222"));
  private static final String CONTENT_1 = "sample file content 1";
  private static final String CONTENT_2 = "another file content 2";

  @Ignore
  @Test
  public void shouldGetExistingObject() throws Exception {
    // given
    ReplicatorWrapper replicator = startNewReplicator()
        .store(ID_1.toString(), CONTENT_1)
        .store(ID_1.toString() + ".meta", "metadata");
    // when
    replicator.sendMsg(new GetObjectsMessage(null, Sets.newHashSet(ID_1), null));
    // then
    Message result = replicator.receiveMsg();
    Assert.assertTrue(result instanceof SendObjectsMessage);
    Assert.assertArrayEquals(CONTENT_1.getBytes(Charsets.UTF_8),
        ((SendObjectsMessage) result).getEncryptedEntities().get(ID_1).getEncryptedData());
    // clean
    endReplicator(replicator);
  }

  @Ignore
  @Test
  public void shouldSendErrorMessageWhenGettingNonexistentObject() throws Exception {
    // given
    ReplicatorWrapper replicator = startNewReplicator();
    // when
    replicator.sendMsg(new GetObjectsMessage(null, Sets.newHashSet(ID_1), null));
    // then
    Message result = replicator.receiveMsg();
    Assert.assertTrue(result instanceof ReplicatorErrorMessage);
    // clean
    endReplicator(replicator);
  }

  @Ignore
  @Test
  public void shouldSaveObject() throws Exception {
    // given
    ReplicatorWrapper replicator = startNewReplicator();
    // when
    storeObject(replicator, ID_1, CONTENT_1);
    // then
    byte[] retrieved = replicator.store_.get(ID_1.toString());
    Assert.assertNotNull("Object not saved", retrieved);
    Assert.assertEquals(CONTENT_1, new String(retrieved, Charsets.UTF_8));
  }

  @Test
  public void shouldDeleteObject() throws Exception {
    // given
    ReplicatorWrapper replicator = startNewReplicator()
        .store(ID_1.toString(), CONTENT_1);
    Assert.assertNotNull("Object not stored", replicator.store_.get(ID_1.toString()));
    // when
    deleteObject(replicator, ID_1);
    // then
    Assert.assertNull("Object not deleted", replicator.store_.get(ID_1.toString()));
  }

  @Ignore
  @Test
  public void shouldIndexStoredFile() throws Exception {
    // given
    ReplicatorWrapper replicator = startNewReplicator();
    // when
    storeObject(replicator, ID_1, CONTENT_1);
    // then
    Set<String> index = replicator.getIndex();
    Assert.assertEquals("Incorrect index size", 1, index.size());
    Assert.assertTrue("Incorrect index content", index.contains(ID_1.toString()));
  }

  @Ignore
  @Test
  public void shouldNotIndexDeletedFile() throws Exception {
    // given
    InMemoryStore<byte[]> store = new InMemoryStore<byte[]>();
    ReplicatorWrapper replicator = startNewReplicator(store);
    storeObject(replicator, ID_1, CONTENT_1);
    replicator = startNewReplicator(store);
    storeObject(replicator, ID_2, CONTENT_2);
    // when
    replicator = startNewReplicator(store);
    deleteObject(replicator, ID_1);
    // then
    Set<String> index = replicator.getIndex();
    Assert.assertEquals("Incorrect index size", 1, index.size());
    Assert.assertTrue("Incorrect index content", index.contains(ID_2.toString()));
  }

  @Ignore
  @Test
  public void shouldHaveMetaData() throws Exception {
    // given
    ReplicatorWrapper replicator = startNewReplicator();
    // when
    storeObject(replicator, ID_1, CONTENT_1);
    // then
    Map<String, MetaData> meta = replicator.getMeta();
    Assert.assertTrue("Does not contain objectID", meta.containsKey(ID_1.toString()));
    Assert.assertEquals("Incorrect file size in meta",
        CONTENT_1.length(), meta.get(ID_1.toString()).getWholeObjectSize().intValue());
  }


  // waits for the replicator thread to end
  private void storeObject(ReplicatorWrapper replicator, ObjectId id, String content)
      throws InterruptedException {
    replicator.sendMsg(new QueryToStoreObjectsMessage("1", null,
        Collections.singletonMap(id, new EncryptedObject(content.getBytes(Charsets.UTF_8))),
        Collections.singletonMap(id, (List<String>) new LinkedList<String>()), "1",
        new HashMap<ObjectId, String>(),
        null, Collections.singletonMap(id, 1)));
    Message reply = replicator.receiveMsg();
    Preconditions.checkArgument(reply instanceof ConfirmationMessage, "Incorrect msg type " +
        reply.getClass());
    replicator.sendMsg(new TransactionResultMessage("1", null, TransactionAnswer.COMMIT));
    endReplicator(replicator);
  }

  private void deleteObject(ReplicatorWrapper replicator, ObjectId id) throws InterruptedException {
    replicator.sendMsg(new DeleteObjectMessage("1", null, id, "1"));
    Message reply = replicator.receiveMsg();
    Preconditions.checkArgument(reply instanceof ConfirmationMessage, "Incorrect msg type " +
        reply.getClass());
    endReplicator(replicator);
  }

  private ReplicatorWrapper startNewReplicator() {
    ReplicatorWrapper wrapper = new ReplicatorWrapper(new InMemoryStore<byte[]>());
    wrapper.thread_.start();
    return wrapper;
  }

  private ReplicatorWrapper startNewReplicator(KeyValueStore<byte[]> store) {
    ReplicatorWrapper wrapper = new ReplicatorWrapper(store);
    wrapper.thread_.start();
    return wrapper;
  }

  private void endReplicator(ReplicatorWrapper wrapper) throws InterruptedException {
    wrapper.thread_.join();
  }



  static class ReplicatorWrapper {
    public KeyValueStore<byte[]> store_;
    public Replicator replicator_;
    public BlockingQueue<Message> inQueue_ = new LinkedBlockingQueue<Message>();
    public BlockingQueue<Message> outQueue_ = new LinkedBlockingQueue<Message>();
    public BlockingQueue<Message> networkQueue_ = new LinkedBlockingQueue<Message>();
    public Thread thread_;

    public ReplicatorWrapper(KeyValueStore<byte[]> store) {
      store_ = store;
      replicator_ = new ReplicatorImpl(store_);
      replicator_.setInQueue(inQueue_);
      replicator_.setOutQueue(outQueue_);
      replicator_.setNetworkQueue(networkQueue_);
      replicator_.cacheStoredObjectsMeta();
      thread_ = new Thread(replicator_);
    }

    public void sendMsg(Message msg) {
      inQueue_.add(msg);
    }

    public Message receiveMsg() throws InterruptedException {
      return networkQueue_.take();
    }

    public ReplicatorWrapper store(String key, String value) throws IOException {
      store_.put(key, value.getBytes(Charsets.UTF_8));
      return this;
    }

    public Set<String> getIndex() {
      return replicator_.getStoredObjectsIds();
    }

    public Map<String, MetaData> getMeta() {
      return replicator_.getStoredMetaData();
    }
  }
}
