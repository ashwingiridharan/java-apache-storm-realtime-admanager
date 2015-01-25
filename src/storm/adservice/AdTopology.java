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
