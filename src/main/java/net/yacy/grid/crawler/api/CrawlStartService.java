/**
 *  CrawlStartService
 *  Copyright 12.6.2017 by Michael Peter Christen, @0rb1t3r
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

package net.yacy.grid.crawler.api;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Date;

import javax.servlet.http.HttpServletResponse;

import org.json.JSONArray;
import org.json.JSONObject;

import ai.susi.mind.SusiAction;
import ai.susi.mind.SusiThought;
import net.yacy.grid.QueueName;
import net.yacy.grid.YaCyServices;
import net.yacy.grid.crawler.Crawler;
import net.yacy.grid.crawler.Crawler.CrawlstartURLSplitter;
import net.yacy.grid.http.APIHandler;
import net.yacy.grid.http.ObjectAPIHandler;
import net.yacy.grid.http.Query;
import net.yacy.grid.http.ServiceResponse;
import net.yacy.grid.io.index.WebMapping;
import net.yacy.grid.io.messages.ShardingMethod;
import net.yacy.grid.mcp.Data;
import net.yacy.grid.tools.JSONList;
import net.yacy.grid.tools.MultiProtocolURL;

/**
 * 
 * Test URL:
 * http://localhost:8300/yacy/grid/crawler/crawlStart.json?crawlingURL=yacy.net&indexmustnotmatch=.*Mitmachen.*&mustmatch=.*yacy.net.*
 */
public class CrawlStartService extends ObjectAPIHandler implements APIHandler {

    private static final long serialVersionUID = 8578474303031749879L;
    public static final String NAME = "crawlStart";
    
    
    @Override
    public String getAPIPath() {
        return "/yacy/grid/crawler/" + NAME + ".json";
    }

    @Override
    public ServiceResponse serviceImpl(Query call, HttpServletResponse response) {
        JSONObject crawlstart = CrawlerDefaultValuesService.crawlStartDefaultClone();
        for (String key: crawlstart.keySet()) {
            	Object object = crawlstart.get(key);
            	if (object instanceof String) crawlstart.put(key, call.get(key, crawlstart.getString(key)));
            	else if (object instanceof Integer) crawlstart.put(key, call.get(key, crawlstart.getInt(key)));
            else if (object instanceof Long) crawlstart.put(key, call.get(key, crawlstart.getLong(key)));
            else if (object instanceof JSONArray) {
                JSONArray a = crawlstart.getJSONArray(key);
                Object cv = call.get(key);
                if (cv != null) crawlstart.put(key, cv);
            }
            	else {
            	    System.out.println("unrecognized type: " + object.getClass().toString());
            	}
        }
        
        // set the crawl id
        CrawlstartURLSplitter crawlstartURLs = new CrawlstartURLSplitter(crawlstart.getString("crawlingURL"));
        Date now = new Date();
        // start the crawls; each of the url in a separate crawl to enforce parallel loading from different hosts
        SusiThought allCrawlstarts = new SusiThought();
        int count = 0;
        for (MultiProtocolURL url: crawlstartURLs.getURLs()) {
            JSONObject singlecrawl = new JSONObject();
            for (String key: crawlstart.keySet()) singlecrawl.put(key, crawlstart.get(key)); // create a clone of crawlstart
            singlecrawl.put("id", Crawler.getCrawlID(url, now, count++));
            //singlecrawl.put("crawlingURLs", new JSONArray().put(url.toNormalform(true)));
            
            try {
                QueueName queueName = Data.gridBroker.queueName(YaCyServices.crawler, YaCyServices.crawler.getQueues(), ShardingMethod.LOOKUP, url.getHost());
                SusiThought json = new SusiThought();
                json.setData(new JSONArray().put(singlecrawl));
                JSONObject action = new JSONObject()
                        .put("type", YaCyServices.crawler.name())
                        .put("queue", queueName.name())
                        .put("id", singlecrawl.getString("id"))
                        .put("depth", 0)
                        .put("sourcegraph", "rootasset");
                SusiAction crawlAction = new SusiAction(action);
                JSONObject graph = new JSONObject(true).put(WebMapping.canonical_s.getSolrFieldName(), url.toNormalform(true));
                crawlAction.setJSONListAsset("rootasset", new JSONList().add(graph));
                json.addAction(crawlAction);
                allCrawlstarts.addAction(crawlAction);
                byte[] b = json.toString().getBytes(StandardCharsets.UTF_8);
                Data.gridBroker.send(YaCyServices.crawler, queueName, b);
            } catch (IOException e) {
                Data.logger.warn("error when starting crawl for " + url.toNormalform(true), e);
                allCrawlstarts.put(ObjectAPIHandler.COMMENT_KEY, e.getMessage());
            }
        }
        
        // construct a crawl start message
        allCrawlstarts.setData(new JSONArray().put(crawlstart));
        allCrawlstarts.put(ObjectAPIHandler.SUCCESS_KEY, allCrawlstarts.getActions().size() > 0);
        
        // finally add the crawl start on the queue
        return new ServiceResponse(allCrawlstarts);
    }

}

