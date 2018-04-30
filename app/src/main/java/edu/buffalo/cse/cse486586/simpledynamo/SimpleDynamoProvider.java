package edu.buffalo.cse.cse486586.simpledynamo;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Formatter;

import android.content.ContentProvider;
import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.net.Uri;
import android.os.AsyncTask;
import android.telephony.TelephonyManager;
import android.util.Log;

import static android.content.ContentValues.TAG;

public class SimpleDynamoProvider extends ContentProvider {
	static final String REMOTE_PORT0 = "11108"; //emulator-5554
	static final String REMOTE_PORT1 = "11112"; //emulator-5556
	static final String REMOTE_PORT2 = "11116"; //emulator-5558
	static final String REMOTE_PORT3 = "11120"; //emulator-5560
	static final String REMOTE_PORT4 = "11124"; //emulator-5562
	static final int SERVER_PORT = 10000;
	static final String INSERT = "insert";

	private String myPort;
	private ArrayList<String> ringOrder;

	@Override
	public int delete(Uri uri, String selection, String[] selectionArgs) {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public String getType(Uri uri) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Uri insert(Uri uri, ContentValues values) {
        Log.v("insert", values.toString());
        String key = values.getAsString("key");
        String value = values.getAsString("value");
        String targetPort = getOwner(key);
        if(targetPort.equals(myPort)){
            storeKVP(key, value);
            return uri;
        }
        new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, INSERT, key, value, targetPort);
		return uri;
	}

    public void storeKVP(String key, String value){
        FileOutputStream outputStream;
        try {
            outputStream = getContext().openFileOutput(key, Context.MODE_PRIVATE);
            outputStream.write(value.getBytes());
            outputStream.close();
        } catch (Exception e) {
            Log.e(TAG, "File write failed");
        }
    }

	@Override
	public boolean onCreate() {
	    //Populate full view of ring (hard-coding order is fine for PA4)
        ringOrder = new ArrayList<String>();
        ringOrder.add("11124");
        ringOrder.add("11112");
        ringOrder.add("11108");
        ringOrder.add("11116");
        ringOrder.add("11120");

		//Get my port
		TelephonyManager tel = (TelephonyManager) getContext().getSystemService(Context.TELEPHONY_SERVICE);
		String portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
		myPort = String.valueOf((Integer.parseInt(portStr) * 2));

        //Boot up our server so we can receive stuff
        try {
            ServerSocket serverSocket = new ServerSocket(SERVER_PORT);
            new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);
        } catch (IOException e) {
            e.printStackTrace();
        }

        //TODO: Logic for recovering nodes
            //Contact successor and recover replicas and missed writes

		return false;
	}

	//Given a key, determines which node is responsible for that key
	public String getOwner(String key) {
        try{
            String keyHash = genHash(key);
            //If keyHash is less than the first node in the ring, that node gets it
            if(keyHash.compareTo(portToNodeID(ringOrder.get(0))) <= 0){
                return ringOrder.get(0);
            }
            //If keyHash is between two nodes, the latter node gets it
            for(int i = 1; i < ringOrder.size(); i++){
                String predID = portToNodeID(ringOrder.get(i-1));
                String curID = portToNodeID(ringOrder.get(i));

                if(keyHash.compareTo(predID) >= 0 && keyHash.compareTo(curID) <= 0){
                    return ringOrder.get(i);
                }
            }
            //If keyHash is greater than the final node, the first node gets it
            return ringOrder.get(0);
        } catch (NoSuchAlgorithmException e){
            e.printStackTrace();
        }
        return "something bad happened in getOwner";
	}


	public String portToNodeID(String port) throws NoSuchAlgorithmException {
		String emuID = "";
		if (port.equals(REMOTE_PORT0)) {
			emuID = "5554";
		} else if (port.equals(REMOTE_PORT1)) {
			emuID = "5556";
		} else if (port.equals(REMOTE_PORT2)) {
			emuID = "5558";
		} else if (port.equals(REMOTE_PORT3)) {
			emuID = "5560";
		} else if (port.equals(REMOTE_PORT4)) {
			emuID = "5562";
		}
		return genHash(emuID);
	}

	@Override
	public Cursor query(Uri uri, String[] projection, String selection,
			String[] selectionArgs, String sortOrder) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public int update(Uri uri, ContentValues values, String selection,
			String[] selectionArgs) {
		// TODO Auto-generated method stub
		return 0;
	}

    private String genHash(String input) throws NoSuchAlgorithmException {
        MessageDigest sha1 = MessageDigest.getInstance("SHA-1");
        byte[] sha1Hash = sha1.digest(input.getBytes());
        Formatter formatter = new Formatter();
        for (byte b : sha1Hash) {
            formatter.format("%02x", b);
        }
        return formatter.toString();
    }

    private class ClientTask extends AsyncTask<String, Void, Void> {

        @Override
        protected Void doInBackground(String... msgs) {

            String msgType = msgs[0];

            try {
                if(msgType.equals(INSERT)){
                    String key = msgs[1];
                    String value = msgs[2];
                    String targetPort = msgs[3];

                    Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(targetPort));
                    String message = msgType + "," + key + "," + value + "," + myPort + '\n';
                    DataOutputStream out = new DataOutputStream(socket.getOutputStream());
                    out.writeBytes(message);
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
            return null;
        }
    }

    private class ServerTask extends AsyncTask<ServerSocket, String, Void> {

        @Override
        protected Void doInBackground(ServerSocket... serverSockets) {
            while(true) {
                ServerSocket serverSocket = serverSockets[0];
                try {
                    Socket socket = serverSocket.accept();
                    Log.e("Socket", "Accepted");
                    DataInputStream in = new DataInputStream(socket.getInputStream());
                    String in_text = in.readLine();
                    in.close();
                    Log.e("Received Text", in_text);
                    String[] message = in_text.split(",");

                    String msgType = message[0];

                    if(msgType.equals(INSERT)){
                        String key = message[1];
                        String value = message[2];
                        String sourcePort = message[3];

                        storeKVP(key, value);
                        //TODO: Code for replication to next two nodes
                    }

                } catch (IOException e) {
                    e.printStackTrace();
                }

            }
        }
    }
}
