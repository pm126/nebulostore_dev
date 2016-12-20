package org.nebulostore.appcore.messaging;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

import org.nebulostore.appcore.exceptions.NebuloException;

/**
 * Generic Message visitor class. Subclasses should define "visit" methods with one parameter that
 * is a subclass of Message. Visitor will go up the class hierarchy and call the first suitable
 * method it finds.
 *
 * @param <R>
 *          return type.
 *
 * @author Bolek Kulbabinski
 */
public abstract class MessageVisitor {
  private static final String METHOD_NAME = "visit";

  /* Common action for all messages that are not handled. */
  protected void visitDefault(Message message) throws NebuloException {
    throw new UnsupportedMessageException(message.getClass().getName() + " in " + getClass());
  }

  public void visit(Object message) throws NebuloException {
    if (message instanceof Message) {
      Class<?> currClass = message.getClass();
      while (currClass != Object.class) {
        Method method = null;
        try {
          method = this.getClass().getMethod(METHOD_NAME, currClass);
        } catch (SecurityException e) {
          throw new RuntimeException("Security exception in visitor.", e);
        } catch (NoSuchMethodException e) {
          currClass = currClass.getSuperclass();
          continue;
        }
        try {
          method.invoke(this, message);
          return;
        } catch (IllegalArgumentException e) {
          throw new RuntimeException("IllegalArgumentException in visitor.", e);
        } catch (IllegalAccessException e) {
          throw new RuntimeException("IllegalAccessException in visitor.", e);
        } catch (InvocationTargetException e) {
          throw new NebuloException("Exception from visit method.", e.getCause());
        }
      }
      visitDefault((Message) message);
    } else {
      throw new UnsupportedMessageException(message.getClass().getName() + " in " + getClass());
    }
  }
}
