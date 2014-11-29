package storm.adservice;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.Queue;
import java.util.Random;
import java.util.concurrent.ConcurrentLinkedQueue;

public class ViewsGenerator
    implements Runnable
{
    String[] locations = { "arizona", "carolina", "texas", "california", "sandiego", "sanjose", "ohio", "columbia",
        "newyork" };

    static Random rand = new Random();

    public static Queue<ViewHolder> viewHolderQueue = new ConcurrentLinkedQueue<ViewHolder>();

    public void generateRandomViews( int intervalSecs )
        throws InterruptedException
    {
        while ( true )
        {
            generateRandomViews();
            Thread.sleep( intervalSecs * 1000 );
        }
    }

    private void generateRandomViews()
    {

        HashMap<String, ArrayList<String>> siteAdMap = AdManager.getSiteAdMapCopy();
        //System.out.println( "in genviews : " + siteAdMap );
        Iterator<Entry<String, ArrayList<String>>> it = siteAdMap.entrySet().iterator();
        while ( it.hasNext() )
        {
            Entry<String, ArrayList<String>> entry = it.next();
            int randomViewCount = rand.nextInt( 100 );
            //System.out.println( "Random View Count : " + randomViewCount );
            for ( int i = 0; i < randomViewCount; i++ )
            {
                ViewHolder vh =
                    new ViewHolder( entry.getKey(), locations[rand.nextInt( locations.length )], entry.getValue() );
                viewHolderQueue.add( vh );
            }
        }

    }

    @Override
    public void run()
    {
        System.out.println( "Triggered Views" );
        try
        {
            generateRandomViews( 1 ); //1
        }
        catch ( InterruptedException e )
        {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

    }

}

class ViewHolder
{
    String siteId;

    String loc;

    ArrayList<String> adIds;

    ViewHolder( String siteId, String loc, ArrayList<String> adIds )
    {
        this.siteId = siteId;
        this.loc = loc;
        this.adIds = adIds;
    }

}
