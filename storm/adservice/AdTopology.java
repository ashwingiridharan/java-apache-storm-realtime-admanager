package storm.adservice;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.CountDownLatch;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;

/**
 * This is a basic example of a Storm topology.
 */
public class AdTopology
{

    private static CountDownLatch latch = new CountDownLatch( 1 );

    public static class SiteViewsBolt
        extends BaseRichBolt
    {
        OutputCollector _collector;

        private HashMap<String, Integer> adCountMap;

        private long prevTime = System.currentTimeMillis() / 5000;

        @Override
        public void prepare( Map conf, TopologyContext context, OutputCollector collector )
        {
            _collector = collector;
            adCountMap = new HashMap<String, Integer>();
        }

        @Override
        public void execute( Tuple tuple )
        {
            String siteId = (String) tuple.getValue( 0 );
            String loc = (String) tuple.getValue( 1 );
            @SuppressWarnings( "unchecked" )
            ArrayList<String> adIds = (ArrayList<String>) tuple.getValue( 2 );
            Integer count;
            for ( String adId : adIds )
            {
                if ( null == ( count = adCountMap.get( adId ) ) )
                {
                    adCountMap.put( adId, 1 );
                }
                else
                {
                    adCountMap.put( adId, count + 1 );
                }
            }

            long currTime = System.currentTimeMillis() / 5000;
            if ( prevTime != currTime )
            {
                AdManager manager = new AdManager();
                HashMap<String, Integer> tmpMap = new HashMap<String, Integer>( adCountMap );
                adCountMap = new HashMap<String, Integer>();
                Iterator<Entry<String, Integer>> it = tmpMap.entrySet().iterator();

                while ( it.hasNext() )
                {
                    Entry<String, Integer> entry = it.next();
                    try
                    {
                        manager.updateViews( entry.getKey(), entry.getValue() );
                    }
                    catch ( InstantiationException | IllegalAccessException | ClassNotFoundException | SQLException e )
                    {
                        // TODO Auto-generated catch block
                        e.printStackTrace();
                    }
                }

                prevTime = currTime;

            }
        }

        @Override
        public void declareOutputFields( OutputFieldsDeclarer declarer )
        {
            declarer.declare( new Fields( "null" ) );
        }

    }

    public static class TrendsInfoBolt
        extends BaseRichBolt
    {
        OutputCollector _collector;

        private HashMap<String, Integer> trendsMap;
        
        private HashMap<String,String> siteContextCache = new HashMap<String, String>();

        private long prevTime = System.currentTimeMillis() / 5000;

        @Override
        public void prepare( Map conf, TopologyContext context, OutputCollector collector )
        {
            _collector = collector;
            trendsMap = new HashMap<String, Integer>();
        }

        @Override
        public void execute( Tuple tuple )
        {
            System.out.println("In execute of Bolt");
            String siteId = (String) tuple.getValue( 0 );
            String loc = (String) tuple.getValue( 1 );
            Integer count = 0;
            String context = "";
            try
            {
                if(null == (context = siteContextCache.get( siteId )))
                {
                    context = AdManager.getManagerInstance().getSiteContext( siteId );
                    siteContextCache.put( siteId, context );
                }
            }
            catch ( InstantiationException | IllegalAccessException | ClassNotFoundException | SQLException e )
            {
                // TODO Auto-generated catch block
                e.printStackTrace();
                context = "";
            }
            String key = siteId + ":" + context + ":" + loc;
            if ( null == ( count = trendsMap.get( key ) ) )
            {
                trendsMap.put( key, 1 );
            }
            else
            {
                trendsMap.put( key, count + 1 );
            }
            long currTime = System.currentTimeMillis() / 5000;
            //System.out.println("TrendsMap1 : "+trendsMap);
            if ( prevTime != currTime )
            {
                AdManager manager = AdManager.getManagerInstance();
                HashMap<String, Integer> tmpMap = new HashMap<String, Integer>( trendsMap );
                trendsMap = new HashMap<String, Integer>();
                Iterator<Entry<String, Integer>> it = tmpMap.entrySet().iterator();
                System.out.println("TrendsMap : "+tmpMap);

                while ( it.hasNext() )
                {
                    Entry<String, Integer> entry = it.next();
                    String[] tokens = entry.getKey().split( ":" );
                    try
                    {
                        if ( tokens.length == 3 )
                        {
                            manager.updateTrends( tokens[0], entry.getValue(), tokens[1], tokens[2] );
                        }
                    }
                    catch ( InstantiationException | IllegalAccessException | ClassNotFoundException | SQLException e )
                    {
                        // TODO Auto-generated catch block
                        e.printStackTrace();
                    }
                }

                prevTime = currTime;

            }

        }

        @Override
        public void declareOutputFields( OutputFieldsDeclarer declarer )
        {
            declarer.declare( new Fields( "null" ) );
        }

    }

    public static void main( String[] args )
        throws Exception
    {
        TopologyBuilder builder = new TopologyBuilder();

        builder.setSpout( "viewSpout", new SiteViewsSpout(), 1 );
        builder.setBolt( "viewBolt", new SiteViewsBolt(), 1 ).shuffleGrouping( "viewSpout" );
        builder.setBolt( "trendsBolt", new TrendsInfoBolt(), 1 ).shuffleGrouping( "viewSpout" );
        // MainTrigger.main( null );

        Config conf = new Config();
        conf.setDebug( true );

        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology( "test", conf, builder.createTopology() );
        latch.await();
        cluster.killTopology( "test" );
        cluster.shutdown();
    }
}
