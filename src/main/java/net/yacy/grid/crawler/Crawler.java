/**
 *  Crawler
 *  Copyright 25.04.2017 by Michael Peter Christen, @0rb1t3r
 *
 *  This library is free software; you can redistribute it and/or
 *  modify it under the terms of the GNU Lesser General Public
 *  License as published by the Free Software Foundation; either
 *  version 2.1 of the License, or (at your option) any later version.
 *
 *  This library is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 *  Lesser General Public License for more details.
 *
 *  You should have received a copy of the GNU Lesser General Public License
 *  along with this program in the file lgpl21.txt
 *  If not, see <http://www.gnu.org/licenses/>.
 */

package net.yacy.grid.crawler;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import javax.servlet.Servlet;

import net.yacy.grid.YaCyServices;
import net.yacy.grid.crawler.api.CrawlStartService;
import net.yacy.grid.crawler.api.CrawlerDefaultValuesService;
import net.yacy.grid.mcp.BrokerListener;
import net.yacy.grid.mcp.Configuration;
import net.yacy.grid.mcp.MCP;
import net.yacy.grid.mcp.Service;
import net.yacy.grid.tools.CronBox;
import net.yacy.grid.tools.CronBox.Telemetry;
import net.yacy.grid.tools.GitTool;
import net.yacy.grid.tools.Logger;

/**
 * The Crawler main class
 *
 * performance debugging:
 * http://localhost:8300/yacy/grid/mcp/info/threaddump.txt
 * http://localhost:8300/yacy/grid/mcp/info/threaddump.txt?count=100
 */
public class Crawler {

    private final static YaCyServices CRAWLER_SERVICE = YaCyServices.crawler;
    private final static String DATA_PATH = "data";

    // define services
    @SuppressWarnings("unchecked")
    public final static Class<? extends Servlet>[] CRAWLER_SERVICES = new Class[]{
            CrawlerDefaultValuesService.class,
            CrawlStartService.class
    };

    public static class Application implements CronBox.Application {

        final Configuration config;
        final Service service;
        final BrokerListener brokerApplication;
        final CronBox.Application serviceApplication;

        public Application() {
            Logger.info("Starting Crawler Application...");
            Logger.info(new GitTool().toString());

            // initialize configuration
            final List<Class<? extends Servlet>> services = new ArrayList<>();
            services.addAll(Arrays.asList(MCP.MCP_SERVLETS));
            services.addAll(Arrays.asList(CRAWLER_SERVICES));
            this.config =  new Configuration(DATA_PATH, true, CRAWLER_SERVICE, services.toArray(new Class[services.size()]));
            final int priorityQueues = Integer.parseInt(this.config.properties.get("grid.indexer.priorityQueues"));
            CrawlerListener.initPriorityQueue(priorityQueues);

            // initialize REST server with services
            this.service = new Service(this.config);

            // connect backend
            this.config.connectBackend();

            // initiate broker application: listening to indexing requests at RabbitMQ
            this.brokerApplication = new CrawlerListener(this.config, CRAWLER_SERVICE);

            // initiate service application: listening to REST request
            this.serviceApplication = this.service.newServer(null);
        }

        @Override
        public void run() {

            Logger.info("Grid Name: " + this.config.properties.get("grid.name"));

            // starting threads
            new Thread(this.brokerApplication).start();
            this.serviceApplication.run(); // SIC! the service application is running as the core element of this run() process. If we run it concurrently, this runnable will be "dead".
        }

        @Override
        public void stop() {
            Logger.info("Stopping Crawler Application...");
            this.serviceApplication.stop();
            this.brokerApplication.stop();
            this.service.stop();
            this.service.close();
            this.config.close();
        }

        @Override
        public Telemetry getTelemetry() {
            return null;
        }

    }

    public static void main(final String[] args) {
        // run in headless mode
        System.setProperty("java.awt.headless", "true"); // no awt used here so we can switch off that stuff

        // prepare configuration
        final Properties sysprops = System.getProperties(); // system properties
        System.getenv().forEach((k,v) -> {
            if (k.startsWith("YACYGRID_")) sysprops.put(k.substring(9).replace('_', '.'), v);
        }); // add also environment variables

        // first greeting
        Logger.info("Crawler started!");
        Logger.info(new GitTool().toString());

        // run application with cron
        final long cycleDelay = Long.parseLong(System.getProperty("YACYGRID_CRAWLER_CYCLEDELAY", "" + Long.MAX_VALUE)); // by default, run only in one genesis thread
        final int cycleRandom = Integer.parseInt(System.getProperty("YACYGRID_CRAWLER_CYCLERANDOM", "" + 1000 * 60 /*1 minute*/));
        final CronBox cron = new CronBox(Application.class, cycleDelay, cycleRandom);
        cron.cycle();

        // this line is reached if the cron process was shut down
        Logger.info("Crawler terminated");
    }

}
