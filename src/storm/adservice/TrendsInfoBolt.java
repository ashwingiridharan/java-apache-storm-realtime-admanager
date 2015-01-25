package storm.adservice;

import java.sql.SQLException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

public class TrendsInfoBolt
    extends BaseRichBolt
{
    OutputCollector _collector;

    private HashMap<String, Integer> trendsMap;

    private HashMap<String, String> siteContextCache = new HashMap<String, String>();

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
        System.out.println( "In execute of Bolt" );
        String siteId = (String) tuple.getValue( 0 );
        String loc = (String) tuple.getValue( 1 );
        Integer count = 0;
        String context = "";
        try
        {
            if ( null == ( context = siteContextCache.get( siteId ) ) )
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
        // System.out.println("TrendsMap1 : "+trendsMap);
        if ( prevTime != currTime )
        {
            AdManager manager = AdManager.getManagerInstance();
            HashMap<String, Integer> tmpMap = new HashMap<String, Integer>( trendsMap );
            trendsMap = new HashMap<String, Integer>();
            Iterator<Entry<String, Integer>> it = tmpMap.entrySet().iterator();
            System.out.println( "TrendsMap : " + tmpMap );

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
