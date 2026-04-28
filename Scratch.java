import com.simplekafka.broker.Protocol;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;

public class Scratch {
    public static void main(String[] args) throws Exception {
        try (SocketChannel channel = SocketChannel.open(new InetSocketAddress("localhost", 9091))) {
            ByteBuffer request = Protocol.encodeCreateTopicRequest("test-topic2", 1, (short)1);
            while (request.hasRemaining()) {
                channel.write(request);
            }
            
            ByteBuffer responseBuffer = ByteBuffer.allocate(1024);
            int bytesRead = channel.read(responseBuffer);
            System.out.println("Bytes read: " + bytesRead);
            if (bytesRead > 0) {
                responseBuffer.flip();
                byte type = responseBuffer.get();
                System.out.println("Response type: " + type + " (Expected: " + Protocol.CREATE_TOPIC_RESPONSE + ", Error: " + Protocol.ERROR_RESPONSE + ")");
            }
        }
    }
}
