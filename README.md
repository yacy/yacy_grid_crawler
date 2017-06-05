# YaCy Grid Component: Crawler

The YaCy Grid is the second-generation implementation of YaCy, a peer-to-peer search engine.
A YaCy Grid installation consists of a set of micro-services which communicate with each other
using the MCP, see https://github.com/yacy/yacy_grid_mcp

## Purpose

The Crawler is a microservices which can be deployed i.e. using Docker. When the Crawler Component
is started, it searches for a MCP and connect to it. By default the local host is searched for a
MCP but you can configure one yourself.

## What it does

The Crawler then does the following:

```
while (a Crawl Contract is in the queue crawler_pending) do
   - read the target url from the contract
   - check against the search index if the url is registered in the transaction index as 'to-be-parsed'. If not, continue
   - load the url content from the assets (it must have been loaded before! - that is another process)
   - parse the content and create a YaCy JSON object with that content
   - place the YaCy JSON within a contract in the index_pending queue
   - extract all links from the YaCy JSON
   - check the validity of the links using the crawl contract
   - all remaining urls are checked against the transaction index, all existing urls are discarded
   - write an index entry for the remaining urls with status 'to-be-loaded'
   - and these remaining urls are placed onto the loader_pending queue
   - the status of the target url is set to to-be-indexed
od
```
## Required Infrastructure (Search Index, Asset Storage and Message Queues)

This requires an transaction index with the following information:
* `URL` (as defined with https://tools.ietf.org/html/rfc3986)
* `crawlid` (a hash)
* status (`to-be-loaded`, `to-be-parsed`, `to-be-indexed`, `indexed`)
As long as a crawl process is running, new urls (as discovered in the html source of a target url)
must be written to the transaction index before the target url has a status change (from to-be-parsed to to-be-indexed).
This makes it possible that the status of a crawl job and the fact that it has been terminted can be
discovered from the transaction index.
* if all status entries for a single `crawlid` are `indexed` then the crawl has been terminated.
The Crawl process needs another database index, which contains the crawl description. The content must be almost the same as
describe in http://www.yacy-websuche.de/wiki/index.php/Dev:APICrawler

Every loader and parser microservice must read this crawl profile information. Because that information is required
many times, we omit a request into the cawler index by adding the crawler profile into each contract of a crawl job in the
crawler_pending and loader_pending queue.

The crawl is therefore controlled by those queues:
* `loader_pending` queue: entries which the yacy_grid_loader process reads. This process loads given resources and writes them to the asset storage.
* `crawler_pending`queue: entries which the yacy_grid_crawler process reads. This process loads the content from the asset storage, parses the content and creates new loader_pending tasks.

The required indexes are:
* a crawl profile index
* a transaction index which reflects the crawl status
* a search index

The microservices will create these indexes on their own using the MCP component.

## Installation: Download, Build, Run
At this time, yacy_grid_crawler is not provided in compiled form, you easily build it yourself. It's not difficult and done in one minute! The source code is hosted at https://github.com/yacy/yacy_grid_crawler, you can download it and run loklak with:

    > git clone --recursive https://github.com/yacy/yacy_grid_crawler.git

If you just want to make a update, do the following

    > git pull origin master
    > git submodule foreach git pull origin master

To build and start the crawler, run

    > cd yacy_grid_crawler
    > gradle run

Please read also https://github.com/yacy/yacy_grid_mcp/edit/master/README.md for further details.

## Contribute

This is a community project and your contribution is welcome!

1. Check for [open issues](https://github.com/yacy/yacy_grid_crawler/issues)
   or open a fresh one to start a discussion around a feature idea or a bug.
2. Fork [the repository](https://github.com/yacy/yacy_grid_crawler.git)
   on GitHub to start making your changes (branch off of the master branch).
3. Write a test that shows the bug was fixed or the feature works as expected.
4. Send a pull request and bug us on Gitter until it gets merged and published. :)

## What is the software license?
LGPL 2.1

Have fun!

@0rb1t3r
