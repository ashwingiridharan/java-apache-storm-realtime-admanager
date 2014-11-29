package storm.adservice;

public class MainTrigger
{
    public static void main( String args[] )
    {
        new Thread( new AdManager() ).start();
        new Thread( new ViewsGenerator() ).start();
    }
}
