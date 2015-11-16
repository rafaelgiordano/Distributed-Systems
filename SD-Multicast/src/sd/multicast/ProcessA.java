/* ================================================================== *

    Universidade Federal de São Carlos - UFSCar, Sorocaba

    Disciplina: Sistemas Distribuídos
    Prof. Fabio Luciano Verdi

    Aplicação que simula o envio de mensagens em multicast e as 
    entrega em ordem para a aplicação.


 * ================================================================== */


import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;
import java.util.StringTokenizer;
import java.util.Map;

public class ProcessA extends Thread {

    /* Definição de constantes utilizadas ao longo do programa. */
    private static final String pid = "1";
    private static final String multicastMessage = "MULTICAST_MESSAGE";
    private static final String ack = "ACK";

    private static ServerSocket welcomeSocket;
    private final Socket socket;

    /* Inteiro que pode ser atualizado atomicamente, garantindo segurança entre as threads. */
    private static final AtomicInteger timestamp = new AtomicInteger(0);

    /* Estrutura de dados onde é guardado <timestamp + pid, message_id>. Ordenação é feita pela chave. */
    private static final TreeMap<String, String> messageQueue = new TreeMap<>();

    /* Estrutura de dados onde é guardado <timestamp + pid, number_of_acks>. Ordenação é feita pela chave. */
    private static final TreeMap<String, Integer> ackQueue = new TreeMap<>();


    /* Construtor da classe, recebe um socket como parâmetro. */
    public ProcessA( Socket socket ) {
        this.socket = socket;
    }

    /* Função principal chamada ao iniciar o processo. Chama os métodos que criam o cliente e o servidor. */
    public static void main( String[] args ) throws Exception {

        createServerSocket();
        startServerSide();
        startClientSide(multicastMessage);

    }

    /**
     * Cria o socket servidor na porta 25001.
     * @throws java.lang.Exception
     */
    public static void createServerSocket() throws Exception {
        welcomeSocket = new ServerSocket(25001);
    }

    /** 
     * Método que cria uma thread e fica escutando por uma requisição do cliente.
     * Quando a requisição chega, uma nova instância da classe é criada, passando o socket
     * de conexão como parâmetro para o construtor. Em seguida, o método run da classe é chamado.
     */
    public synchronized static void startServerSide() {

        (new Thread() {
            @Override
            public void run() {
                try {

                    while (true) {
                        Socket connectionSocket = welcomeSocket.accept();

                        ProcessA server = new ProcessA(connectionSocket);
                        server.start();
                    }

                } catch ( IOException e ) {
     //               e.printStackTrace();
                }
            }
        }).start();

    }

    /** 
     * Método que cria uma thread e envia duas mensagens para cada processo (B e C).
     * Caso o parâmetro não seja uma mensagem multicast, significa que é necessário mandar um ACK
     * para os outros dois processo
     * @param messageType
     */
    public synchronized static void startClientSide( String messageType ) {

        (new Thread() {
            @Override
            public void run() {
                try {

                    if ( !messageType.equals(multicastMessage) ) {
                        
                        /* Foi passado como parâmetro o id da mensagem que deve ser agradecida. */
                        /* Logo há uma mensagem ACK a ser enviada para os outros dois processos. */
                        
                        /* Criação da mensagem ACK. */
                        String ackMessage = ack + "@" + pid + "@" + timestamp.toString() + "@" + messageType;

                        try (Socket clientSocketB = new Socket("localhost", 25002)) {
                            BufferedWriter outToProcessB = new BufferedWriter(new OutputStreamWriter(clientSocketB.getOutputStream()));
                            
                            /* Manda o ACK para o processo B. */
                            outToProcessB.write(ackMessage);
                            outToProcessB.newLine();
                            outToProcessB.flush();
                        }

                        try (Socket clientSocketC = new Socket("localhost", 25003)) {
                            BufferedWriter outToProcessC = new BufferedWriter(new OutputStreamWriter(clientSocketC.getOutputStream()));
                            
                            /* Manda o ACK para o processo C. */
                            outToProcessC.write(ackMessage);
                            outToProcessC.newLine();
                            outToProcessC.flush();
                        }

                    }
                    else {

                        /* O parâmetro indica que deve ser mandada uma mensagem multicast para os outros dois processos. */

                        /* Thread client dorme por 2 segundos, dando tempo para o servidor do processo B subir. */
                        Thread.sleep(2000);
                        String message_id;
                        String message;

                        Socket clientSocketC;
                        BufferedWriter outToProcessC;
                        try (Socket clientSocketB = new Socket("localhost", 25002)) {
                            BufferedWriter outToProcessB = new BufferedWriter(new OutputStreamWriter(clientSocketB.getOutputStream()));
                            /* Incremento do timestamp. */
                            timestamp.incrementAndGet();
                            /* Criação da mensagem MULTICAST */
                            message_id = "MA" + timestamp.toString();
                            message = multicastMessage + "@" + pid + "@" + timestamp.toString() + "@" + message_id;
                            // System.out.println("\t\tmensagem sendo enviada para ProcessB: " + message);
                            
                            /* Manda a mensagem multicast para o processo B. */
                            outToProcessB.write(message);
                            outToProcessB.newLine();
                            outToProcessB.flush();
                            clientSocketC = new Socket("localhost", 25003);
                            outToProcessC = new BufferedWriter(new OutputStreamWriter(clientSocketC.getOutputStream()));
                            /* Manda a mensagem multicast para o processo C. */
                            outToProcessC.write(message);
                            outToProcessC.newLine();
                            outToProcessC.flush();
                            // System.out.println("\t\tmensagem sendo enviada para ProcessC: " + message);
                            
                            /* Mensagem multicast é mandada conceitualmente para o seu remetente. */
                            messageQueue.put(Integer.toString(timestamp.intValue()) + pid, message_id);
                            ackQueue.put(Integer.toString(timestamp.intValue()) + pid, 0);
                            Thread.sleep(2000);
                            /* Incremento do timestamp. */
                            timestamp.incrementAndGet();
                            message_id = "MA" + timestamp.toString();
                            message = multicastMessage + "@" + pid + "@" + timestamp.toString() + "@" + message_id;
                            // System.out.println("\t\tmensagem sendo enviada para ProcessB: " + message);
                            
                            /* Manda a mensagem multicast para o processo B. */
                            outToProcessB.write(message);
                            outToProcessB.newLine();
                            outToProcessB.flush();
                        }

                        // System.out.println("\t\tmensagem sendo enviada para ProcessC: " + message);

                        /* Manda a mensagem multicast para o processo C. */
                        outToProcessC.write(message);
                        outToProcessC.newLine();
                        outToProcessC.flush();
                        clientSocketC.close();

                        /* Mensagem multicast é mandada conceitualmente para o seu remetente. */
                        messageQueue.put(Integer.toString(timestamp.intValue()) + pid, message_id);
                        ackQueue.put(Integer.toString(timestamp.intValue()) + pid, 0);
                        
                    }

                } catch (UnknownHostException e) {
//                    e.printStackTrace();
                } catch (IOException | InterruptedException e) {
 //                   e.printStackTrace();
                }
                //                  e.printStackTrace();
                
            }
        }).start();

    }

    /** 
     * Método run da classe, trata as mensagens recebidas por um cliente.
     */
    @Override
    public void run() {

        try {
            BufferedReader inFromClient = new BufferedReader(new InputStreamReader(socket.getInputStream()));
            String line;

            while ( (line = inFromClient.readLine()) != null ) {
                /* Quebra a string em tokens, separando o tipo da mensagem, o remetente, o timestamp e o id da mensagem */
                StringTokenizer st = new StringTokenizer(line, "@");
                
                String messageType = st.nextToken();
                String sender = st.nextToken();
                int timestampReceived = Integer.parseInt(st.nextToken());
                String message_id = st.nextToken();

                System.out.print("+ Mensagem recebida: " + messageType);
                System.out.print(". id: " + message_id);
                System.out.print(". remetente: " + sender);
                System.out.print(". timestamp: " + timestampReceived);
                System.out.println(".");

                String key = Integer.toString(timestampReceived) + sender;

                if ( messageType.equals(multicastMessage) ) {

                    /* Mensagem multicast, deve ser guardada na fila de mensagens. */

                    messageQueue.put(key, message_id);
                    ackQueue.put(key, 1);

                    /* Atualiza o timestamp pegando o máximo. */
                    int actualTimestamp = timestamp.intValue();
                    timestamp.set((actualTimestamp > timestampReceived) ? actualTimestamp + 1 : timestampReceived + 1);

                    /* ACK deve ser enviado em multicast. */
                    startClientSide(message_id);

                }
                else {
                    
                    /* ACK recebido. O número de ACKs da mensagem deve ser incrementado. */
                    /* Caso o número de ACKs tenha chegado em 2, e a mensagem está no início da fila, */
                    /* então a mensagem deve ser entregue à aplicação. */
                    
                    Integer numberOfAcks;
                    String firstKey = messageQueue.firstKey();

                    for ( Map.Entry<String, String> entry : messageQueue.entrySet() ) {
                        if ( entry.getValue().equals(message_id) ) {
                            key = entry.getKey();
                            break;
                        }
                    }
                    
                    if ( (numberOfAcks = ackQueue.get(key)) != null ) {

                        /* Incremente o número de ACKs recebidos. */
                        ackQueue.replace(key, numberOfAcks, ++numberOfAcks);

                        if ( numberOfAcks == 2 ) {
                            /* Possui dois ACKs. Necessário verificar se está no início da fila. */
                            
                            if ( firstKey.equals(key) ) {
                                /* Retire da fila e entregue para a aplicação. */
                                Map.Entry<String, String> m = messageQueue.pollFirstEntry();
                                ackQueue.pollFirstEntry();

                                System.out.print("\t - Mensagem entregue para a aplicacao.");
                                System.out.println(" remetente: " + m.getKey() + ". id: " + m.getValue());
                            }

                        }
                    }
                }
            }
        }
        catch ( IOException e ) {
//            e.printStackTrace();
        }

    }

}
