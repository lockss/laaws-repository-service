/*

 Copyright (c) 2017-2019 Board of Trustees of Leland Stanford Jr. University,
 all rights reserved.

 Permission is hereby granted, free of charge, to any person obtaining a copy
 of this software and associated documentation files (the "Software"), to deal
 in the Software without restriction, including without limitation the rights
 to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 copies of the Software, and to permit persons to whom the Software is
 furnished to do so, subject to the following conditions:

 The above copyright notice and this permission notice shall be included in
 all copies or substantial portions of the Software.

 THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.  IN NO EVENT SHALL
 STANFORD UNIVERSITY BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY,
 WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR
 IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.

 Except as contained in this notice, the name of Stanford University shall not
 be used in advertising or otherwise to promote the sale, use or other dealings
 in this Software without prior written authorization from Stanford University.

 */
package org.lockss.laaws.rs;

import static org.lockss.app.LockssApp.PARAM_START_PLUGINS;
import static org.lockss.app.ManagerDescs.*;
import org.lockss.app.LockssApp;
import org.lockss.app.LockssApp.AppSpec;
import org.lockss.app.LockssApp.ManagerDesc;
import org.lockss.app.LockssDaemon;
import org.lockss.app.ServiceDescr;
import org.lockss.log.L4JLogger;
import org.lockss.plugin.PluginManager;
import org.lockss.spring.base.BaseSpringBootApplication;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.ExitCodeGenerator;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.security.SecurityAutoConfiguration;
import org.springframework.boot.autoconfigure.solr.SolrAutoConfiguration;
import org.springframework.context.annotation.ComponentScan;
import springfox.documentation.swagger2.annotations.EnableSwagger2;

/**
 * The Spring-Boot application.
 */
// TODO: This disables Spring authentication
@SpringBootApplication(exclude = {SecurityAutoConfiguration.class, SolrAutoConfiguration.class})
@EnableSwagger2
@ComponentScan(basePackages = { "org.lockss.laaws.rs", "org.lockss.laaws.rs.api" })
public class RepositoryApplication extends BaseSpringBootApplication
	implements CommandLineRunner {
  private static L4JLogger log = L4JLogger.getLogger();

  // Manager descriptors.  The order of this table determines the order in
  // which managers are initialized and started.
  private static final ManagerDesc[] myManagerDescs = {
  };

  /**
   * The entry point of the application.
   *
   * @param args
   *          A String[] with the command line arguments.
   */
  public static void main(String[] args) {
    log.info("Starting the application");
    configure();

    // Start the REST service.
    SpringApplication.run(RepositoryApplication.class, args);
  }

  /**
   * Callback used to run the application starting the LOCKSS daemon.
   *
   * @param args
   *          A String[] with the command line arguments.
   */
  public void run(String... args) {
    // Check whether there are command line arguments available.
    if (args != null && args.length > 0) {
      // Yes: Start the LOCKSS daemon.
      log.info("Starting the LOCKSS Repository Service");

      AppSpec spec = new AppSpec()
	.setService(ServiceDescr.SVC_REPO)
	.setArgs(args)
	.addAppConfig(PARAM_START_PLUGINS, "false")
	.addAppDefault(PluginManager.PARAM_START_ALL_AUS, "false")
	.setAppManagers(myManagerDescs);
      LockssApp.startStatic(LockssDaemon.class, spec);
    } else {
      // No: Do nothing. This happens when a test is started and before the
      // test setup has got a chance to inject the appropriate command line
      // parameters.
    }
  }
}
