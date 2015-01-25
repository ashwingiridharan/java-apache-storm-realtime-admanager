package storm.adservice;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

public class SiteViewsBolt
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
