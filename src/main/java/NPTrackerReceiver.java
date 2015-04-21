/**
 * Created by Andres on 21/04/2015.
 */

import com.keysolutions.ddpclient.DDPClient;
import org.apache.spark.*;
import org.apache.spark.api.java.function.*;
import org.apache.spark.streaming.*;
import org.apache.spark.streaming.api.java.*;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.receiver.Receiver;
import java.net.URISyntaxException;


public class NPTrackerReceiver extends Receiver<String> {

    String host = "";
    Integer port = 0;

    public NPTrackerReceiver(String host_, int port_){
        super(StorageLevel.MEMORY_AND_DISK_2());
        host = host_;
        port = port_;
    }

    public void onStart() {
        // Start the thread that receives data over a connection
        new Thread()  {
            @Override public void run() {
                receive();
            }
        }.start();
    }

    public void onStop() {
        // There is nothing much to do as the thread calling receive()
        // is designed to stop by itself isStopped() returns false
    }

    private void receive() {
        try {
            DDPClient ddp = new DDPClient(host, port);
            DDPObserver obs = new DDPObserver(this);
            ddp.addObserver(obs);
            ddp.connect();
        } catch (URISyntaxException e) {
            restart("URISyntaxException");
        }
    }

}
