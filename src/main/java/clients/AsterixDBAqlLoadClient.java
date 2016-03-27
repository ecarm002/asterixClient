package clients;

import java.io.File;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.HashMap;

import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.util.EntityUtils;

import structures.Query;

public class AsterixDBAqlLoadClient extends AbstractAsterixDBClient {
//    private final static int ASTX_QUERY_PORT = 19001;
    private final static int ASTX_QUERY_PORT = 18002;
    private final static String ASTX_QUERY_URL_SUFFIX = "/aql";

    static URIBuilder roBuilder;
    static DefaultHttpClient httpclient;
    static HttpGet httpGet;

    public static void main(String args[]) {
        if (args.length != 2) {
            System.out.println("Wrong/Insufficient set of arguments.\n" + "Correct Usage:\n" + "args[0]: CC Url\n"
                    + "args[1]: Path to the query load file\n");
            return;
        }
        String ccUrl = args[0];
        String queryFilePath = args[1];

        init(ccUrl);

        if (qSeq == null) {
            qSeq = new ArrayList<>();
        }
        qSeq.clear();
        if (idToQuery == null) {
            idToQuery = new HashMap<String, Query>();
        }
        idToQuery.clear();
        File f = new File(queryFilePath);
        loadQuery(f);
        qSeq.add(new String(f.getName()));

        for (String nextQ : qSeq) {
            Query q = idToQuery.get(nextQ);
            System.out.println("\nStarting query " + q);
            long rspt = executeQueryLoad(q);
            System.out.println("Query " + q.getName() + " took " + rspt + " ms");
        }

        terminate();
        System.out.println("\nAll Done successfully ! ");
    }

    private static long executeQueryLoad(Query q) {
        String content = null;
        long rspTime = -1L; // initial value
        try {
            roBuilder.setParameter("query", q.getBody());
            URI uri = roBuilder.build();
            httpGet.setURI(uri);

            long s = System.currentTimeMillis(); //Start the timer right before sending the query
            HttpResponse response = httpclient.execute(httpGet); //Actual execution against the server
            HttpEntity entity = response.getEntity(); //Extract response
            content = EntityUtils.toString(entity); //We make sure we extract the results from response
            long e = System.currentTimeMillis(); //Stop the timer
            EntityUtils.consume(entity);

            rspTime = (e - s); //Total duration
        } catch (Exception ex) {
            System.err.println("Problem in read-only query execution against Asterix\n" + content);
            ex.printStackTrace();
            return -1L; //invalid time (as the query crashed)
        }
        return rspTime;
    }

    private static void init(String ccUrl) {
        try {
            roBuilder = new URIBuilder("http://" + ccUrl + ":" + ASTX_QUERY_PORT + ASTX_QUERY_URL_SUFFIX);
            httpclient = new DefaultHttpClient();
            httpGet = new HttpGet();
        } catch (URISyntaxException e) {
            System.err.println("Issue(s) in initializing the HTTP client");
            e.printStackTrace();
        }
    }

    private static void terminate() {
        if (httpclient != null) {
            httpclient.getConnectionManager().shutdown();
        }
    }
}