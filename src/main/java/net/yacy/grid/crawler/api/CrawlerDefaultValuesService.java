/**
 *  CrawlerDefaultValuesService
 *  Copyright 04.6.2017 by Michael Peter Christen, @0rb1t3r
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

import javax.servlet.http.HttpServletResponse;

import org.json.JSONObject;

import net.yacy.grid.http.APIHandler;
import net.yacy.grid.http.ObjectAPIHandler;
import net.yacy.grid.http.Query;
import net.yacy.grid.http.ServiceResponse;

/**
 * 
 * Test URL:
 * http://localhost:8300/yacy/grid/crawler/defaultValues.json
 * 
 * Test command:
 * curl http://localhost:8300/yacy/grid/crawler/defaultValues.json
 */
public class CrawlerDefaultValuesService extends ObjectAPIHandler implements APIHandler {

    private static final long serialVersionUID = 8578474303031749879L;
    public static final String NAME = "defaultValues";
    
    @Override
    public String getAPIPath() {
        return "/yacy/grid/crawler/" + NAME + ".json";
    }

    public static JSONObject crawlStartDefault() {
    	JSONObject json = new JSONObject(true);

        json.put("crawlingMode", "url");
        json.put("crawlingURL", "");
        json.put("sitemapURL", "");
        json.put("crawlingFile", "");
        json.put("crawlingDepth", 3);
        json.put("crawlingDepthExtension", "");
        json.put("range", "domain");
        json.put("mustmatch", ".*");
        json.put("mustnotmatch", "");
        json.put("ipMustmatch", ".*");
        json.put("ipMustnotmatch", "");
        json.put("indexmustmatch", ".*");
        json.put("indexmustnotmatch", "");
        json.put("deleteold", "off");
        json.put("deleteIfOlderNumber", 0);
        json.put("deleteIfOlderUnit", "day");
        json.put("recrawl", "nodoubles");
        json.put("reloadIfOlderNumber", 0);
        json.put("reloadIfOlderUnit", "day");
        json.put("crawlingDomMaxCheck", "off");
        json.put("crawlingDomMaxPages", 1000);
        json.put("crawlingQ", "off");
        json.put("directDocByURL", "off");
        json.put("storeHTCache", "off");
        json.put("cachePolicy", "if fresh");
        json.put("indexText", "on");
        json.put("indexMedia", "off");
        json.put("xsstopw", "off");
        json.put("collection", "user");
        json.put("agentName", "");
        return json;
    }
    
    @Override
    public ServiceResponse serviceImpl(Query call, HttpServletResponse response) {
        return new ServiceResponse(crawlStartDefault());
    }

}

