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

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Stream;

import javax.servlet.Servlet;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.json.JSONTokener;

import ai.susi.mind.SusiAction;
import net.yacy.grid.YaCyServices;
import net.yacy.grid.crawler.api.CrawlerDefaultValuesService;
import net.yacy.grid.io.assets.Asset;
import net.yacy.grid.io.index.WebMapping;
import net.yacy.grid.mcp.AbstractBrokerListener;
import net.yacy.grid.mcp.BrokerListener;
import net.yacy.grid.mcp.Data;
import net.yacy.grid.mcp.MCP;
import net.yacy.grid.mcp.Service;
import net.yacy.grid.tools.MultiProtocolURL;


public class Crawler {

    private final static YaCyServices SERVICE = YaCyServices.crawler;
    private final static String DATA_PATH = "data";
    private final static String APP_PATH = "crawler";
 
    // define services
    @SuppressWarnings("unchecked")
    public final static Class<? extends Servlet>[] CRAWLER_SERVICES = new Class[]{
            CrawlerDefaultValuesService.class
    };

    private final static String[] FIELDS_IN_GRAPH = new String[]{
            WebMapping.inboundlinks_sxt.name(),
            WebMapping.outboundlinks_sxt.name(),
            WebMapping.images_sxt.name(),
            WebMapping.images_alt_sxt.name(),
            WebMapping.frames_sxt.name(),
            WebMapping.iframes_sxt.name()
    };
    
    private final static Set<MultiProtocolURL> doubles = new HashSet<>();
    /**
     * broker listener, takes process messages from the queue "crawler", "webcrawler"
     * i.e. test with:
     * curl -X POST -F "message=@job.json" -F "serviceName=crawler" -F "queueName=webcrawler" http://yacygrid.com:8100/yacy/grid/mcp/messages/send.json
     * where job.json is:
{
  "metadata": {
    "process": "yacy_grid_loader",
    "count": 1
  },
  "data": [{
    "id": "201705042045000-xyz",
    "crawlingMode": "url",
    "crawlingURL": "http://yacy.net",
    "sitemapURL": "",
    "crawlingFile": "",
    "crawlingDepth": 3,
    "crawlingDepthExtension": "",
    "range": "domain",
    "mustmatch": ".*",
    "mustnotmatch": "",
    "ipMustmatch": ".*",
    "ipMustnotmatch": "",
    "indexmustmatch": ".*",
    "indexmustnotmatch": "",
    "deleteold": "off",
    "deleteIfOlderNumber": 0,
    "deleteIfOlderUnit": "day",
    "recrawl": "nodoubles",
    "reloadIfOlderNumber": 0,
    "reloadIfOlderUnit": "day",
    "crawlingDomMaxCheck": "off",
    "crawlingDomMaxPages": 1000,
    "crawlingQ": "off",
    "directDocByURL": "off",
    "storeHTCache": "off",
    "cachePolicy": "if fresh",
    "indexText": "on",
    "indexMedia": "off",
    "xsstopw": "off",
    "collection": "user",
    "agentName": "yacybot (yacy.net; crawler from yacygrid.com)",
    "user": "anonymous@nowhere.com",
    "client": "yacygrid.com"
  }],
  "actions": [{
    "type": "crawler",
    "queue": "webcrawler",
    "id": "201705042045000-xyz",
    "depth": 1,
    "sourcegraph": "test3/yacy.net.graph.json"
  }]
}

{
  "metadata": {
    "process": "yacy_grid_parser",
    "count": 1
  },
  "data": [{"collection": "test"}],
  "actions": [{
    "type": "parser",
    "queue": "yacyparser",
    "sourceasset": "test3/yacy.net.warc.gz",
    "targetasset": "test3/yacy.net.text.jsonlist",
    "targetgraph": "test3/yacy.net.graph.jsonlist",
    "actions": [
      {
      "type": "indexer",
      "queue": "elasticsearch",
      "targetindex": "webindex",
      "targettype" : "common",
      "sourceasset": "test3/yacy.net.text.jsonlist"
      },
      {
        "type": "crawler",
        "queue": "webcrawler",
        "sourceasset": "test3/yacy.net.graph.jsonlist"
      }
    ]
  }]
}
     */
    
    
    public static class CrawlerListener extends AbstractBrokerListener implements BrokerListener {

        @Override
        public boolean processAction(SusiAction action, JSONArray data) {
            String id = action.getStringAttr("id");
            if (id == null || id.length() == 0) {
                Data.logger.info("Fail: Action does not have an id: " + action.toString());
                return false;
            }
            JSONObject crawl = selectCrawlData(data, id);
            if (crawl == null) {
                Data.logger.info("Fail: ID of Action not found in data: " + action.toString());
                return false;
            }
            
            int depth = action.getIntAttr("depth");
            if (depth == 0) {
                // this is a crawl start
                // construct the loading, parsing, indexing action
                // we take the start url from the data object
                String crawlingURL = crawl.getString("crawlingURL");
                String[] urls = crawlingURL.split(" ");
                // start urls are never checked against the double list
                // we need file paths for the storage. So all we have here is the start url, and the id
                
                
            } else {
                // this is a follow-up
                String sourcegraph = action.getStringAttr("sourcegraph");
                if (sourcegraph == null || sourcegraph.length() == 0) {
                    Data.logger.info("Fail: sourcegraph of Action is empty: " + action.toString());
                    return false;
                }
                try {
                    Asset<byte[]> graphasset = Data.gridStorage.load(sourcegraph); // this must be a list of json, containing document links
                    ByteArrayInputStream bais = new ByteArrayInputStream(graphasset.getPayload());
                    BufferedReader br = new BufferedReader(new InputStreamReader(bais, StandardCharsets.UTF_8));
                    String line;
                    graphloop: while ((line = br.readLine()) != null) try {
                        JSONObject json = new JSONObject(new JSONTokener(line));
                        if (json.has("index")) continue graphloop; // this is an elasticsearch index directive, we just skip that
                        
                        //String url = json.getString(WebMapping.url_s.name());
                        Set<MultiProtocolURL> graph = new HashSet<>();
                        if (json.has(WebMapping.canonical_s.name())) graph.add(new MultiProtocolURL(json.getString(WebMapping.canonical_s.name())));
                        for (String field: FIELDS_IN_GRAPH) {
                            if (json.has(field)) {
                                JSONArray a = json.getJSONArray(field);
                                for (int i = 0; i < a.length(); i++) graph.add(new MultiProtocolURL(a.getString(i)));
                            }
                        }
                        
                        // sort out doubles
                        List<MultiProtocolURL> next = new ArrayList<>();
                        graph.forEach(url -> {
                            if (!Crawler.doubles.contains(url)) {
                                Crawler.doubles.add(url);
                                next.add(url);
                            }
                        });
                        
                        
                    } catch (JSONException je) {}
                    
                    Data.logger.info("processed message from queue and loaded graph " + sourcegraph);
                    return true;
                } catch (IOException e) {
                    Data.logger.info("Fail: loading of sourcegraph failed: " + e.getMessage() + "\n" + action.toString(), e);
                    return false;
                }
            }
            
            return false;
        }
        
        private static JSONObject selectCrawlData(JSONArray data, String id) {
            for (int i = 0; i < data.length(); i++) {
                JSONObject json = data.getJSONObject(i);
                if (json.has("id") && json.get("id").equals(id)) return json;
            }
            return null;
        }
    }

    public static void main(String[] args) {
        BrokerListener brokerListener = new CrawlerListener();
        new Thread(brokerListener).start();
        List<Class<? extends Servlet>> services = new ArrayList<>();
        services.addAll(Arrays.asList(MCP.MCP_SERVICES));
        services.addAll(Arrays.asList(CRAWLER_SERVICES));
        Service.runService(SERVICE, DATA_PATH, APP_PATH, null, services);
        brokerListener.terminate();
    }
    
}
