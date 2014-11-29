java-apache-storm-realtime-admanager
====================================

A simple ad placement manager using Apache Storm. More features to be added.

Overview

Every advertisement will have a set of contexts. For example, the ad for "reebok shoe" will have the context as "sports/gears".
Similarly every website will have a context for itself. For example, the site espncricinfo.com has the context  "sports/cricket" and sayflipkart.com will have the context as "gadgets", "laptops", "furnitures" etc.
If the reebok ad is placed on espn site, then it is context based ad, as both the AD context and publisher context matches.
 
Now the parties involved here are advertisers ( reebok shoe company), publishers ( site owners) and ad agency( say Google).
 
Now there has to be a decision system to help Google to place the right ads in right websites for the money paid.
 
The reachability of an AD is based on the number of hits it gained from the web users.
 
Now say there are three levels of budget for advertisers offered by Google, tier A, tier B and tier C for 24 HOURS time period.
Tier A guarantees that atleast million users watches it and atleast 100 users clicks it.
Tier B guarantees that atleast half million users watches it and atleast 50 users clicks it.
Tier C guarantees that atleast 1/4 million users watches it and atleast 25 users clicks it.
 
Advertisers can choose any of the tier and pay for it.
 
Now to achieve the hits promised to the advertisers, Google should find the top sites which yields maximum hits based on contexts. To find the appropriate sites and dynamically place the relevant ads, Google needs a decision system.
 
The decision system (DS) will determine the trending sites or popular sites at the MOMENT, which will be used for AD placement.
The following information will be gathered,
1. Which site is getting maximum hits at the moment?
2. Which site is getting maximum hits for a particular context?
3. Which location users are more interested in a particular context?
 
Use Case:
Now say reebook registered for tier B in google, which requires 5,00,000 views and 50 hits.
Now say after 20 hours, only 4,00,000 views and 30 hits were there for reebok ad. So, google has to ensure that 20 hits has to be done in next 4 hours. To do that, google have to place the rebook AD in sites, which are more popular at the moment and having relevant contexts.
 
To help this real time dynamic decision making, we will use our STORM system.












API Details (Component wise)

Database : MySql or Cassandra

AdInfo ( AdId, AdDetails, Context, ExpectedViews, TotalViews, ExpectedHits, TotalHits, TimeEnd)
SiteInfo ( SiteId, SitelName, Context)
TrendsInfo ( TimeFrame, SiteId, Context, Loc, Views, Hits)

Simulator (Java)

AdRegistration
SiteInfoRegistration
AdPlacementManager
GraphInfoFetcher

Spouts (Emits tuples to Bolts)

SiteViewsSpout - Generates( {views, SiteId, Loc, AdIds = [AdId1, AdId2, AdId3, .. , AdIdn]} ) - Ads viewed by the user in his session
SiteHitsSpout - Generates( {hits, SiteId, Loc, AdId} ) - Ad clicked by the user

Bolts (Stores timeline statistics in database)

TrendInfoBolt listens SiteViewsSpout ,SiteHitsSpout - Outputs/Updates DB( TimeFrame, SiteId, Context, Loc, Views, Hits)
AdDistributorBolt listens SiteViewsSpout - Outputs/Updates DB (views, AdId, 1)
AdTrendsBolt listens AdDistributorBolt, SiteHitsSpout - Outputs/UpdatesDB (hits, AdId, 1)

