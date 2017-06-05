/**
 *  Loader
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

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import javax.servlet.Servlet;

import org.json.JSONObject;
import org.json.JSONTokener;

import ai.susi.mind.SusiAction;
import ai.susi.mind.SusiThought;
import net.yacy.grid.YaCyServices;
import net.yacy.grid.crawler.api.CrawlerDefaultValuesService;
import net.yacy.grid.io.messages.MessageContainer;
import net.yacy.grid.mcp.Data;
import net.yacy.grid.mcp.MCP;
import net.yacy.grid.mcp.Service;


public class Crawler {

    private final static YaCyServices SERVICE = YaCyServices.crawler;
    private final static String DATA_PATH = "data";
    private final static String APP_PATH = "crawler";
 
    // define services
    @SuppressWarnings("unchecked")
    public final static Class<? extends Servlet>[] CRAWLER_SERVICES = new Class[]{
            CrawlerDefaultValuesService.class
    };
    
    /**
     * broker listener, takes process messages from the queue "crawler", "webcrawler"
     * i.e. test with:
     * curl -X POST -F "message=@job.json" -F "serviceName=crawler" -F "queueName=webcrawler" http://yacygrid.com:8100/yacy/grid/mcp/messages/send.json
     * where job.json is:
{
  "metadata": {
    "process": "yacy_grid_crawler",
    "count": 1
  },
  "data": [{"collection": "test"}],
  "actions": [{
    "type": "crawler",
    "queue": "webcrawler",
    "urls": ["http://yacy.net"],
    "collection": "test",
    "targetasset": "test3/yacy.net.warc.gz"
  }
  ]
}
     */
    public static class BrokerListener extends Thread {
        public boolean shallRun = true;
        
        @Override
        public void run() {
            while (shallRun) {
                if (Data.gridBroker == null) {
                    try {Thread.sleep(1000);} catch (InterruptedException ee) {}
                } else try {
                    MessageContainer<byte[]> mc = Data.gridBroker.receive(YaCyServices.crawler.name(), YaCyServices.crawler.getDefaultQueue(), 10000);
                    if (mc == null || mc.getPayload() == null) continue;
                    JSONObject json = new JSONObject(new JSONTokener(new String(mc.getPayload(), StandardCharsets.UTF_8)));
                    SusiThought process = new SusiThought(json);
                    List<SusiAction> actions = process.getActions();
                    if (!actions.isEmpty()) {
                        SusiAction a = actions.get(0);
                        String targetasset = a.getStringAttr("targetasset");
                    }
                } catch (IOException e) {
                    e.printStackTrace();
                    try {Thread.sleep(1000);} catch (InterruptedException ee) {}
                }
            }
        }
        public void terminate() {
            this.shallRun = false;
        }
    }
    
    public static void main(String[] args) {
        BrokerListener brokerListener = new BrokerListener();
        brokerListener.start();
        List<Class<? extends Servlet>> services = new ArrayList<>();
        services.addAll(Arrays.asList(MCP.MCP_SERVICES));
        services.addAll(Arrays.asList(CRAWLER_SERVICES));
        Service.runService(SERVICE, DATA_PATH, APP_PATH, null, services);
        brokerListener.terminate();
    }
    
}
