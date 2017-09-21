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

import javax.servlet.http.HttpServletResponse;

import org.json.JSONArray;
import org.json.JSONObject;

import ai.susi.mind.SusiAction;
import ai.susi.mind.SusiThought;
import net.yacy.grid.QueueName;
import net.yacy.grid.YaCyServices;
import net.yacy.grid.crawler.Crawler.CrawlstartURLs;
import net.yacy.grid.http.APIHandler;
import net.yacy.grid.http.ObjectAPIHandler;
import net.yacy.grid.http.Query;
import net.yacy.grid.http.ServiceResponse;
import net.yacy.grid.io.messages.ShardingMethod;
import net.yacy.grid.mcp.Data;

/**
 * 
 * Test URL:
 * http://localhost:8300/yacy/grid/crawler/crawlStart.json?crawlingURL=yacy.net
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
        	if (object instanceof Integer) crawlstart.put(key, call.get(key, crawlstart.getInt(key)));
        	if (object instanceof Long) crawlstart.put(key, call.get(key, crawlstart.getLong(key)));
        }
        
        // set the crawl id
        CrawlstartURLs crawlstartURLs = new CrawlstartURLs(crawlstart.getString("crawlingURL"));
        crawlstart.put("id", crawlstartURLs.getId());
        crawlstart.put("crawlingURLs", crawlstartURLs.getURLs());
        
        // construct a crawl start message
        SusiThought json = new SusiThought();
        json.setData(new JSONArray().put(crawlstart));
        try {
            QueueName queueName = Data.gridBroker.queueName(YaCyServices.crawler, YaCyServices.crawler.getQueues(), ShardingMethod.LOOKUP, crawlstartURLs.getHashKey());
            JSONObject action = new JSONObject()
            	.put("type", YaCyServices.crawler.name())
            	.put("queue", queueName.name())
            	.put("id", crawlstart.getString("id"))
            	.put("depth", 0);
            json.addAction(new SusiAction(action));
            
            // put the crawl message on the queue
            byte[] b = json.toString().getBytes(StandardCharsets.UTF_8);
            Data.gridBroker.send(YaCyServices.crawler, queueName, b);
			json.put(ObjectAPIHandler.SUCCESS_KEY, true);
		} catch (IOException e) {
			e.printStackTrace();
			json.put(ObjectAPIHandler.SUCCESS_KEY, false);
			json.put(ObjectAPIHandler.COMMENT_KEY, e.getMessage());
		}
        
        // finally add the crawl start on the queue
        return new ServiceResponse(json);
    }

}

