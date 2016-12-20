package org.nebulostore.peers;

import java.util.List;

import com.google.inject.AbstractModule;
import com.google.inject.name.Names;

import org.apache.commons.configuration.XMLConfiguration;
import org.apache.commons.configuration.tree.ConfigurationNode;
import org.apache.log4j.Logger;

/**
 * Generic Guice Module capable of reading XML configuration file and adding all properties as
 * bindings.
 *
 * @author Bolek Kulbabinski
 */
public abstract class GenericConfiguration extends AbstractModule {
  private static Logger logger_ = Logger.getLogger(GenericConfiguration.class);

  // TODO(bolek): Change to private and not use in subclasses.
  protected XMLConfiguration config_;

  public void setXMLConfig(XMLConfiguration config) {
    config_ = config;
  }

  @Override
  protected void configure() {
    bindPropertiesFromXML();
    configureAll();
  }

  protected abstract void configureAll();

  private void bindPropertiesFromXML() {
    bindRecursively(config_.getRoot(), "");
  }

  private void bindRecursively(ConfigurationNode node, String parentName) {
    String name = parentName + "." + node.getName();
    List<ConfigurationNode> children = node.getChildren();
    if (children.size() == 0) {
      String val = (String) node.getValue();
      String defaultType = "String";
      if (node.getAttributeCount() > 0) {
        ConfigurationNode attribute = node.getAttribute(0);
        defaultType = (String) attribute.getValue();
      }
      bindSingle(name, defaultType, val);
    } else {
      for (ConfigurationNode child : children) {
        bindRecursively(child, name);
      }
    }
  }

  private void bindSingle(String fullName, String type, String val) {
    // Remove ".peer." prefix.
    int secondDot = fullName.indexOf('.', 1);
    String name = fullName.substring(secondDot + 1, fullName.length());
    logger_.info("Binding " + name + " of type " + type + " to value " + val);
    if ("Integer".equals(type)) {
      bindConstant().annotatedWith(Names.named(name)).to(Integer.valueOf(val));
    } else if ("String".equals(type)) {
      bindConstant().annotatedWith(Names.named(name)).to(val);
    } else if ("Boolean".equals(type)) {
      bindConstant().annotatedWith(Names.named(name)).to(Boolean.valueOf(val));
    } else {
      throw new IllegalArgumentException("Unsupported type " + type);
    }
  }
}

