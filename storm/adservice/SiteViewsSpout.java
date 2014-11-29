package storm.adservice;

import backtype.storm.Config;
import backtype.storm.topology.OutputFieldsDeclarer;
import java.util.Map;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;
import java.util.HashMap;
import org.apache.log4j.Logger;

public class SiteViewsSpout
    extends BaseRichSpout
{
    /**
     * 
     */
    private static final long serialVersionUID = 1L;

    public static Logger LOG = Logger.getLogger( SiteViewsSpout.class );

    boolean _isDistributed;

    SpoutOutputCollector _collector;

    public SiteViewsSpout()
    {
        this( true );
    }

    public SiteViewsSpout( boolean isDistributed )
    {
        _isDistributed = isDistributed;
    }

    public void open( Map conf, TopologyContext context, SpoutOutputCollector collector )
    {
        _collector = collector;
        new Thread( new AdManager() ).start();
        new Thread( new ViewsGenerator() ).start();
    }

    public void close()
    {

    }

    public void nextTuple()
    {
        Utils.sleep( 1 );
        ViewHolder vh = ViewsGenerator.viewHolderQueue.poll();
        if ( null != vh )
        {
            System.out.println( "Value Emitted" );
            _collector.emit( new Values( vh.siteId, vh.loc, vh.adIds ) );
        }
        else
        {
            System.out.println( "No Elem in Queue" );
            Utils.sleep( 100 );
        }
    }

    public void ack( Object msgId )
    {

    }

    public void fail( Object msgId )
    {

    }

    public void declareOutputFields( OutputFieldsDeclarer declarer )
    {
        declarer.declare( new Fields( "siteid", "location", "adids" ) );
    }

    @Override
    public Map<String, Object> getComponentConfiguration()
    {
        if ( !_isDistributed )
        {
            Map<String, Object> ret = new HashMap<String, Object>();
            ret.put( Config.TOPOLOGY_MAX_TASK_PARALLELISM, 1 );
            return ret;
        }
        else
        {
            return null;
        }
    }
}
