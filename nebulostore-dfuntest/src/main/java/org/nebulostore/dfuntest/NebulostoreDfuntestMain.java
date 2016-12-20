package org.nebulostore.dfuntest;
import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import com.google.inject.TypeLiteral;
import com.google.inject.multibindings.Multibinder;
import com.google.inject.name.Named;
import com.google.inject.name.Names;

import me.gregorias.dfuntest.ApplicationFactory;
import me.gregorias.dfuntest.Environment;
import me.gregorias.dfuntest.EnvironmentFactory;
import me.gregorias.dfuntest.EnvironmentPreparator;
import me.gregorias.dfuntest.LocalEnvironmentFactory;
import me.gregorias.dfuntest.MultiTestRunner;
import me.gregorias.dfuntest.SSHEnvironmentFactory;
import me.gregorias.dfuntest.TestResult;
import me.gregorias.dfuntest.TestRunner;
import me.gregorias.dfuntest.TestScript;
import me.gregorias.dfuntest.testrunnerbuilders.GuiceTestRunnerModule;

import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.HierarchicalConfiguration;
import org.apache.commons.configuration.XMLConfiguration;
import org.apache.commons.configuration.tree.ConfigurationNode;
import org.nebulostore.crypto.CryptoUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Main class used for booting dfuntest's tests.
 *
 * @author Grzegorz Milka
 */
public final class NebulostoreDfuntestMain {
  private static final Logger LOGGER = LoggerFactory.getLogger(NebulostoreDfuntestMain.class);

  private static final String CONFIG_OPTION = "config";
  private static final String CONFIG_FILE_OPTION = "config-file";
  private static final String ENV_FACTORY_OPTION = "env-factory";
  private static final String HELP_OPTION = "help";

  private static final String ENV_DIR_PREFIX = "NebulostoreDfuntest";
  private static final String REPORT_PATH_PREFIX = "report_";
  private static final String TEST_SCRIPT_ARGUMENT_NAME = "test-script";
  private static final String TEST_ID_ARGUMENT_NAME = "dfuntest.test-id";

  private static final Map<String, String> DEFAULT_PROPERTIES = newDefaultProperties();

  private static Class< ? extends EnvironmentFactory<Environment>> environmentFactoryClass_;

  /**
   * Runs dfuntests or displays help message.
   *
   * To display help message use --help option.
   *
   * To run use --env-factory (local|ssh) to specify environment setting and use --config-file PATH
   * and/or --config (KEY=VALUE)* to provide necessary initialization variables.
   *
   * This runner uses a combination of guice injections and config, argument parsing for
   * initialization. That is argument names of dfuntest classes are annotated with names.
   * To provide custom value to such argument on runtime you have to either write it
   * into config-file in specific format or provide as a --config argument.
   *
   * @param args
   */
  @SuppressWarnings("unchecked")
  public static void main(String[] args) {
    environmentFactoryClass_ = null;
    Map<String, String> properties = new HashMap<>(DEFAULT_PROPERTIES);
    Map<String, String> argumentProperties;
    try {
      argumentProperties = parseAndProcessArguments(args);
    } catch (ConfigurationException | IllegalArgumentException | ParseException e) {
      System.exit(1);
      return;
    }

    if (argumentProperties == null) {
      return;
    }

    properties.putAll(argumentProperties);
    properties.put(TEST_ID_ARGUMENT_NAME, CryptoUtils.getRandomString());

    if (environmentFactoryClass_ == null) {
      LOGGER.error("main(): EnvironmentFactory has not been set.");
      System.exit(1);
      return;
    }

    GuiceTestRunnerModule guiceBaseModule = new GuiceTestRunnerModule();
    guiceBaseModule.addProperties(properties);

    Class<TestScript<NebulostoreApp>> testScriptClass;
    try {
      testScriptClass = (Class<TestScript<NebulostoreApp>>)
          Class.forName(properties.get(TEST_SCRIPT_ARGUMENT_NAME));
    } catch (ClassNotFoundException | ClassCastException e) {
      LOGGER.error("Wrong test script class.", e);
      System.exit(1);
      return;
    }

    NebulostoreGuiceModule guiceNebulostoreModule = new NebulostoreGuiceModule(testScriptClass);

    Injector injector = Guice.createInjector(guiceBaseModule, guiceNebulostoreModule);
    TestRunner testRunner = injector.getInstance(TestRunner.class);

    TestResult result = testRunner.run();

    int status;
    String resultStr;
    if (result.getType() == TestResult.Type.SUCCESS) {
      status = 0;
      resultStr = "successfully";
    } else {
      status = 1;
      resultStr = "with failure";
    }
    LOGGER.info("main(): Test has ended {} with description: {}", resultStr,
        result.getDescription());
    System.exit(status);
  }
  private static class NebulostoreGuiceModule extends AbstractModule {

    private final Class<TestScript<NebulostoreApp>> testScriptClass_;

    public NebulostoreGuiceModule(Class<TestScript<NebulostoreApp>> testScriptClass) {
      testScriptClass_ = testScriptClass;
    }

    @Override
    protected void configure() {
      bind(new AppFactoryTypeLiteral()).to(NebulostoreAppFactory.class).in(Singleton.class);
      bind(new EnvironmentPreparatorTypeLiteral()).to(NebulostoreEnvironmentPreparator.class)
          .in(Singleton.class);
      bind(new EnvironmentFactoryTypeLiteral()).to(environmentFactoryClass_).in(Singleton.class);
      Multibinder<TestScript<NebulostoreApp>> multiBinder = Multibinder.newSetBinder(binder(),
          new TestScriptTypeLiteral(), Names.named(MultiTestRunner.SCRIPTS_ARGUMENT_NAME));
      multiBinder.addBinding().to(testScriptClass_);
      bind(TestRunner.class).to(new MultiTestRunnerTypeLiteral()).in(Singleton.class);
    }

    @Provides
    @Named(SSHEnvironmentFactory.EXECUTOR_ARGUMENT_NAME)
    @Singleton
    Executor provideSSHEnvironmentFactoryExecutor() {
      return Executors.newCachedThreadPool();
    }

    private static class AppFactoryTypeLiteral
        extends TypeLiteral<ApplicationFactory<Environment, NebulostoreApp>> {
    }

    private static class EnvironmentFactoryTypeLiteral
        extends TypeLiteral<EnvironmentFactory<Environment>> {
    }

    private static class EnvironmentPreparatorTypeLiteral
        extends TypeLiteral<EnvironmentPreparator<Environment>> {
    }

    private static class TestScriptTypeLiteral extends TypeLiteral<TestScript<NebulostoreApp>> {
    }
  }

  private static class MultiTestRunnerTypeLiteral extends
      TypeLiteral<MultiTestRunner<Environment, NebulostoreApp>> {
  }

  private static String calculateCurrentTimeStamp() {
    return new SimpleDateFormat("yyyyMMdd-HHmmssSSS").format(new Date());
  }

  private static Path calculateReportPath() {
    return FileSystems.getDefault().getPath(REPORT_PATH_PREFIX + calculateCurrentTimeStamp());
  }

  @SuppressWarnings("unchecked")
  private static Map<String, String> constructPropertiesFromRootsChildren(
      HierarchicalConfiguration config) {
    Map<String, String> properties = new HashMap<>();
    List<ConfigurationNode> rootsChildrenList = config.getRoot().getChildren();
    for (ConfigurationNode child : rootsChildrenList) {
      HierarchicalConfiguration subConfig = config.configurationAt(child.getName());
      properties.putAll(GuiceTestRunnerModule.configurationToProperties(subConfig));
    }
    return properties;
  }

  private static Options createOptions() {
    Options options = new Options();

    OptionBuilder.withLongOpt(CONFIG_OPTION);
    OptionBuilder.hasArgs();
    OptionBuilder.withValueSeparator(' ');
    OptionBuilder.withDescription("Configure initial dependencies. Arguments should be a list of" +
        " the form: a.b=value1 a.c=value2.");
    Option configOption = OptionBuilder.create();

    OptionBuilder.withLongOpt(CONFIG_FILE_OPTION);
    OptionBuilder.hasArg();
    OptionBuilder.withDescription("XML configuration filename.");
    Option configFileOption = OptionBuilder.create();

    OptionBuilder.withLongOpt(ENV_FACTORY_OPTION);
    OptionBuilder.hasArg();
    OptionBuilder.withDescription("Environment factory name. Can be either local or ssh.");

    Option envFactoryOption = OptionBuilder.create();

    options.addOption(configOption);
    options.addOption(configFileOption);
    options.addOption(envFactoryOption);
    options.addOption("h", HELP_OPTION, false, "Print help.");
    return options;
  }

  private static Map<String, String> newDefaultProperties() {
    Map<String, String> properties = new HashMap<>();
    properties.put(LocalEnvironmentFactory.DIR_PREFIX_ARGUMENT_NAME, ENV_DIR_PREFIX);
    properties.put(SSHEnvironmentFactory.REMOTE_DIR_ARGUMENT_NAME, ENV_DIR_PREFIX);
    properties.put(MultiTestRunner.SHOULD_PREPARE_ARGUMENT_NAME, "true");
    properties.put(MultiTestRunner.SHOULD_CLEAN_ARGUMENT_NAME, "true");
    properties.put(MultiTestRunner.REPORT_PATH_ARGUMENT_NAME, calculateReportPath().toString());
    return properties;
  }

  private static Map<String, String> parseAndProcessArguments(String[] args)
      throws ConfigurationException, ParseException {
    Map<String, String> properties = new HashMap<>();
    CommandLineParser parser = new BasicParser();
    Options options = createOptions();
    CommandLine cmd;
    try {
      cmd = parser.parse(options, args);
    } catch (ParseException e) {
      LOGGER.error("parseAndProcessArguments(): ParseException caught parsing arguments.", e);
      throw e;
    }

    if (cmd.hasOption(HELP_OPTION)) {
      HelpFormatter formatter = new HelpFormatter();
      formatter.printHelp("ExampleGuiceMain", options);
      return null;
    }

    if (cmd.hasOption(CONFIG_FILE_OPTION)) {
      String argValue = cmd.getOptionValue(CONFIG_FILE_OPTION);
      XMLConfiguration config = new XMLConfiguration();
      config.setDelimiterParsingDisabled(true);
      try {
        config.load(argValue);
      } catch (ConfigurationException e) {
        LOGGER.error("parseAndProcessArguments(): ConfigurationException caught when reading" +
            " configuration file.", e);
        throw e;
      }
      properties.putAll(constructPropertiesFromRootsChildren(config));
    }

    if (cmd.hasOption(CONFIG_OPTION)) {
      String[] argValues = cmd.getOptionValues(CONFIG_OPTION);
      for (String arg : argValues) {
        String[] keyAndValue = arg.split("=", 2);
        String key = keyAndValue[0];
        String value = "";
        if (keyAndValue.length == 2) {
          value = keyAndValue[1];
        }
        properties.put(key, value);
      }
    }

    if (cmd.hasOption(ENV_FACTORY_OPTION)) {
      String argValue = cmd.getOptionValue(ENV_FACTORY_OPTION);
      switch (argValue) {
        case "local":
          environmentFactoryClass_ = LocalEnvironmentFactory.class;
          break;
        case "ssh":
          environmentFactoryClass_ = SSHEnvironmentFactory.class;
          break;
        default:
          String errorMsg = "Unknown environment factory " + argValue;
          LOGGER.error("parseAndProcessArguments(): {}", errorMsg);
          throw new IllegalArgumentException(errorMsg);
      }
    } else {
      String errorMsg = String.format("--%s option is required. Use --help to get usage help.",
          ENV_FACTORY_OPTION);
      LOGGER.error("parseAndProcessArguments(): {}", errorMsg);
      throw new IllegalArgumentException(errorMsg);
    }

    return properties;
  }

  private NebulostoreDfuntestMain() {
  }
}
