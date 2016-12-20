package org.nebulostore.dfuntest.coding.api;

import org.nebulostore.api.GetNebuloObjectModule;
import org.nebulostore.appcore.messaging.MessageVisitor;
import org.nebulostore.dfuntest.coding.communication.messages.SendObjectMessageWithSize;

public class GetNebuloObjectModuleWithSize extends GetNebuloObjectModule {

  @Override
  protected MessageVisitor createVisitor() {
    return new GetWithSizeMessageVisitor();
  }

  protected class GetWithSizeMessageVisitor extends StateMachineVisitor {

    public void visit(SendObjectMessageWithSize message) {
      visit(message.getMessage());
    }
  }

}
