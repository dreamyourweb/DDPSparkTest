/**
 * Created by Andres on 21/04/2015.
 */

import java.util.Observable;
import java.util.Observer;
import java.util.Map;
import org.json.*;

import com.keysolutions.ddpclient.DDPClient;
import com.keysolutions.ddpclient.DDPListener;

public class DDPObserver extends DDPListener implements Observer {

    NPTrackerReceiver receiver;

    public DDPObserver(NPTrackerReceiver receiver_) {
        receiver = receiver_;
    }

    @SuppressWarnings("unchecked")
    public void update(Observable client, Object msg) {

        Map<String, Object> jsonFields = (Map<String, Object>) msg;
        String message = (String) jsonFields.get("msg");
//        System.out.println(jsonFields);

        DDPClient meteor = (DDPClient) client;

        if (message.equals("connected")) {
            meteor.subscribe("twitter-np", new Object[] {}, this);
        }

        if (message.equals("added")) {

//            System.out.println("ADDED");
            Map<String, Object> fields = (Map<String, Object>) jsonFields.get("fields");
            Map<String, Object> tweet = (Map<String, Object>) fields.get("tweet");
//            System.out.println((String) tweet.get("text"));
//
//            System.out.println(jsonFields.get("fields"));
//            System.out.println(jsonObject);

            receiver.store((String) tweet.get("text"));
        }
    }

}
