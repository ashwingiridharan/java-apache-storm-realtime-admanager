package storm.adservice;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;

public class AdManager
    implements Runnable
{
    private static HashMap<String, ArrayList<String>> siteAdMap;

    private static AdManager adManager = new AdManager();

    private Connection connection;

    private void placeAds()
        throws Exception
    {
        siteAdMap = new HashMap<String, ArrayList<String>>();
        ResultSet rs = getSiteIdNContext();
        while ( rs.next() )
        {
            String context = rs.getString( 2 );
            String siteId = rs.getString( 1 );
            ResultSet adRes = getCurrentAds( context );
            ArrayList<String> adList = null;
            if ( null == ( adList = siteAdMap.get( siteId ) ) )
            {
                adList = new ArrayList<String>();
            }
            while ( adRes.next() )
            {
                adList.add( adRes.getString( 1 ) );
            }
            siteAdMap.put( siteId, adList );
        }
        System.out.println( siteAdMap );
    }

    public void placeAds( int intervalInSecs )
        throws Exception
    {
        System.out.println( "Triggered manager" );
        while ( true )
        {
            placeAds();
            System.out.println( ViewsGenerator.viewHolderQueue );
            Thread.sleep( intervalInSecs * 1000 );
        }
    }

    public static HashMap<String, ArrayList<String>> getSiteAdMapCopy()
    {
        return new HashMap<String, ArrayList<String>>( siteAdMap );
    }

    private ResultSet getSiteIdNContext()
        throws InstantiationException, IllegalAccessException, ClassNotFoundException, SQLException
    {
        Statement st = getConnection().createStatement();
        ResultSet rs = st.executeQuery( "select id, context from siteinfo" );
        return rs;
    }

    public String getSiteContext( String siteId )
        throws InstantiationException, IllegalAccessException, ClassNotFoundException, SQLException
    {
        Statement st = getConnection().createStatement();
        ResultSet rs = st.executeQuery( "select context from siteinfo where id='" + siteId + "'" );
        if ( rs.next() )
        {
            return rs.getString( 1 );
        }
        else
        {
            return "";
        }
    }

    public static AdManager getManagerInstance()
    {
        return adManager;
    }

    private synchronized Connection getConnection()
        throws InstantiationException, IllegalAccessException, ClassNotFoundException, SQLException
    {
        if ( null == connection )
        {
            Class.forName( "com.mysql.jdbc.Driver" ).newInstance();
            Connection con = DriverManager.getConnection( "jdbc:mysql://localhost:3306/addb", "root", "root" );
            connection = con;
            return con;
        }
        else
        {
            return connection;
        }
    }

    private ResultSet getCurrentAds( String context )
        throws SQLException, InstantiationException, IllegalAccessException, ClassNotFoundException
    {
        Statement st1 = getConnection().createStatement();
        ResultSet rs1 =
            st1.executeQuery( "select id from adinfo where (totviews <= exviews) and context='" + context
                + "' order by (totviews/exviews*100) limit 3;" );
        return rs1;
    }

    public synchronized void updateViews( String adId, int totViews )
        throws SQLException, InstantiationException, IllegalAccessException, ClassNotFoundException
    {
        PreparedStatement st =
            getConnection().prepareStatement( "update adinfo set totviews = totviews + ? where id = ? and totviews <= exviews" );
        st.setInt( 1, totViews );
        st.setString( 2, adId );
        int rs = st.executeUpdate();
        if ( rs != 0 )
        {
            System.out.println( "Value updated" );
        }
    }

    public synchronized void updateTrends( String siteId, int views, String context, String location )
        throws SQLException, InstantiationException, IllegalAccessException, ClassNotFoundException
    {
        
        PreparedStatement st =
            getConnection().prepareStatement( "update trendsinfo set views = views + ? where siteid = ? and context = ? and location = ?" );
        st.setInt( 1, views );
        st.setString( 2, siteId );
        st.setString( 3, context );
        st.setString( 4, location );
        int rs = st.executeUpdate();
        if ( rs != 0 )
        {
            //System.out.println( "Value Updation success" );
            //System.out.println("In update trends *******************************************************");
        }
        else
        {
            //System.out.println("In update trends ++++++++++++++++++++++++++++++++++++++++++++++++++++++++");
            PreparedStatement st1 =
                getConnection().prepareStatement( "insert into trendsinfo (siteid, context, location, views) values (?,?,?,?)" );
            st1.setString( 1, siteId );
            st1.setString( 2, context );
            st1.setString( 3, location );
            st1.setInt( 4, views );
            st1.executeUpdate();
            //System.out.println("In update trends "+rs1);
        }
    }

    @Override
    public void run()
    {
        try
        {
            placeAds( 5 ); //5
        }
        catch ( Exception e )
        {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

    }
}
