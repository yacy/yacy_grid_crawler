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
    
    public static JSONObject defaultValues = new JSONObject(true);
    static {
        defaultValues.put("crawlingMode", "url");
        defaultValues.put("crawlingURL", "");
        defaultValues.put("sitemapURL", "");
        defaultValues.put("crawlingFile", "");
        defaultValues.put("crawlingDepth", 3);
        defaultValues.put("crawlingDepthExtension", "");
        defaultValues.put("range", "domain");
        defaultValues.put("mustmatch", ".*");
        defaultValues.put("mustnotmatch", ".*\\.(js|css|jpg|jpeg|png|dmg|mpg|mpeg|zip|gz|exe|pkg)");
        defaultValues.put("ipMustmatch", ".*");
        defaultValues.put("ipMustnotmatch", "");
        defaultValues.put("indexmustmatch", ".*");
        defaultValues.put("indexmustnotmatch", "");
        defaultValues.put("deleteold", "off");
        defaultValues.put("deleteIfOlderNumber", 0);
        defaultValues.put("deleteIfOlderUnit", "day");
        defaultValues.put("recrawl", "nodoubles");
        defaultValues.put("reloadIfOlderNumber", 0);
        defaultValues.put("reloadIfOlderUnit", "day");
        defaultValues.put("crawlingDomMaxCheck", "off");
        defaultValues.put("crawlingDomMaxPages", 1000);
        defaultValues.put("crawlingQ", "off");
        defaultValues.put("cachePolicy", "if fresh");
        defaultValues.put("collection", "user");
        defaultValues.put("agentName", "");
        defaultValues.put("priority", 0);
        defaultValues.put("loaderHeadless", "true");
    }
    
    @Override
    public String getAPIPath() {
        return "/yacy/grid/crawler/" + NAME + ".json";
    }

    public static JSONObject crawlStartDefaultClone() {
    	JSONObject json = new JSONObject(true);
    	defaultValues.keySet().forEach(key -> json.put(key, defaultValues.get(key)));
        return json;
    }
    
    @Override
    public ServiceResponse serviceImpl(Query call, HttpServletResponse response) {
        return new ServiceResponse(defaultValues);
    }

}

