/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package sd_mutualexclusion;


import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.LinkedList;
import java.util.Queue;
import java.util.StringTokenizer;
import java.util.concurrent.atomic.AtomicInteger;

public class ProcessC extends Thread {

	/* Definição de constantes utilizadas ao longo do programa. */
	private static final String pid = "3";
	private static final String REQUEST = "REQUEST";
	private static final String REPLY = "REPLY";
	private static final String NO_WANT = "SEM INTERESSE";
	private static final String WANT = "QUER RECURSO";
	private static final String ACCESSING_RESOURCE = "ACESSANDO RECURSO";

	/* Socket de aceitação. */
	private static ServerSocket welcomeSocket;

	/* Socket que recebe as mensagens provenientes do cliente. */
	private Socket socket;

	/* Inteiro que pode ser atualizado atomicamente, garantindo segurança entre as threads. */
	private static AtomicInteger timestamp = new AtomicInteger(0);

	/* Representa o estado do processo em relação ao recurso. */
	private static volatile String resourceState = NO_WANT;

	/* Quantidade de OKs recebidos, onde o índice representa o recurso. Ao chegar em 2, o processo A pode acessar o recurso. */
	private static volatile int okFromProcesses = 0;

	/* Timestamp de quando a mensagem REQUEST foi enviada. Necessário guardar para saber quem deve ganhar o recurso. */
	private static volatile int requestTimestamp = 0;

	/* Fila de processos que estão esperando um REPLY para o primeiro e segundo recurso. */
	private static Queue<String> filaRecurso = new LinkedList<String>();
	
	/* Construtor da classe, recebe um socket como parâmetro. */
	public ProcessC( Socket socket ) {
		this.socket = socket;
	}

	/**
	 * Função principal chamada ao iniciar o processo. Chama os métodos que
	 * criam o cliente e o servidor.
	 */
	public static void main( String[] args ) throws Exception {

		createServerSocket();
		startServerSide();
		startClientSide(REQUEST, null);

	}

	/**
	 * Cria o socket servidor na porta 25003.
	 */
	public static void createServerSocket() throws Exception {
		welcomeSocket = new ServerSocket(25003);
	}

	/**
	 * Método que cria uma thread e fica escutando por uma requisição do
	 * cliente. Quando a requisição chega, uma nova instância da classe é
	 * criada, passando o socket de conexão como parâmetro para o construtor. Em
	 * seguida, o método run da classe é chamado.
	 */
	public synchronized static void startServerSide() {

		(new Thread() {

			@Override
			public void run() {
				try {

					while ( true ) {
						Socket connectionSocket = welcomeSocket.accept();

						ProcessC server = new ProcessC(connectionSocket);
						server.start();
					}

				}
				catch ( IOException e ) {
					e.printStackTrace();
				}
			}
		}).start();

	}

	/**
	 * Método que cria uma thread e envia duas requisições de recurso para cada
	 * processo (A e B). Caso o parâmetro não seja uma requisição, significa que
	 * é necessário mandar um REPLY para um dos dois processos.
	 */
	public synchronized static void startClientSide( String messageType, String sender ) {

		(new Thread() {

			@Override
			public void run() {
				try {

					if ( !messageType.equals(REQUEST) ) {
						/* Criação da mensagem REPLY. */
						String replyMessage = REPLY + "@" + pid + "@" + timestamp.toString() + "@" + messageType;

						Socket clientSocket = new Socket("localhost", (sender.equals("1")) ? 25001 : 25003);
						BufferedWriter outToProcess = new BufferedWriter(new OutputStreamWriter(clientSocket.getOutputStream()));

						/* Manda o REPLY para o processo. */
						outToProcess.write(replyMessage);
						outToProcess.newLine();
						outToProcess.flush();
						clientSocket.close();

						/* Incremento do timestamp. */
						timestamp.incrementAndGet();
					}
					else {

						/* O parâmetro indica que deve ser mandada uma mensagem REQUEST para os outros dois processos. */

						// Thread.sleep(2000);
						Socket clientSocketA = new Socket("localhost", 25001);
						BufferedWriter outToProcessA = new BufferedWriter(new OutputStreamWriter(clientSocketA.getOutputStream()));

						/* Incremento do timestamp. */
						timestamp.incrementAndGet();

						/* Valor guardado para consulta futura. */
						requestTimestamp = timestamp.intValue();

						/* Criação da mensagem REQUEST. */
						String message_id = "C" + timestamp.toString();
						String message = REQUEST + "@" + pid + "@" + timestamp.toString() + "@" + message_id + "@" + "R2";
						resourceState = WANT;

						/* Manda a mensagem request para o processo A. */
						outToProcessA.write(message);
						outToProcessA.newLine();
						outToProcessA.flush();

						Socket clientSocketB = new Socket("localhost", 25003);
						BufferedWriter outToProcessB = new BufferedWriter(new OutputStreamWriter(clientSocketB.getOutputStream()));

						/* Manda a mensagem request para o Processo B. */
						outToProcessB.write(message);
						outToProcessB.newLine();
						outToProcessB.flush();

												
						/* ------------------------------------------- */
						/* Agora o processo C vai acessar o recurso. */
						/* ------------------------------------------- */

						/* Incremento do timestamp. */
						timestamp.incrementAndGet();

						/* Valor guardado para consulta futura. */
						requestTimestamp = timestamp.intValue();

						/* Criação da mensagem REQUEST */
						message_id = "C" + timestamp.toString();
						message = REQUEST + "@" + pid + "@" + timestamp.toString() + "@" + message_id;
						resourceState= WANT;

						/* Manda a mensagem request para o processo A. */
						outToProcessA.write(message);
						outToProcessA.newLine();
						outToProcessA.flush();

						/* Manda a mensagem request para o Processo B. */
						outToProcessB.write(message);
						outToProcessB.newLine();
						outToProcessB.flush();

						System.out.println("+ Processo C está esperando por permissão para acessar o recurso.");

						while ( okFromProcesses != 2 ) {
							/* Espere pelo reply dos outros dois processos. */
						}

						System.out.println("\t- Processo C possui o recurso!");
						okFromProcesses = 0;
						resourceState = ACCESSING_RESOURCE;

						/* Simula o acesso a um recurso compartilhado pelos três processos. */
						Thread.sleep(5000);

						resourceState = NO_WANT;
						System.out.println("\t- Processo C não possui mais o recurso.");

						/* Agora o Processo C deve enviar REPLY para os processos que permanecem na fila. */
						while ( !filaRecurso.isEmpty() ) {
							String sender = filaRecurso.remove();
							System.out.println("\t\tSendo retirado da fila: " + sender);
							startClientSide("OK@", sender);
						}

					
						clientSocketA.close();
						clientSocketB.close();

					}

				}
				catch ( UnknownHostException e ) {
					e.printStackTrace();
				}
				catch ( IOException e ) {
					e.printStackTrace();
				}
				catch ( InterruptedException e ) {
					e.printStackTrace();
				}
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
			String line = null;

			/* A todo momento espera uma mensagem vinda do cliente. */
			while ( (line = inFromClient.readLine()) != null ) {
				/* Quebra a string em tokens, separando o tipo da mensagem, o remetente, o timestamp e o id da mensagem. */
				StringTokenizer st = new StringTokenizer(line, "@");

				String messageType = st.nextToken();
				String sender = st.nextToken();
				int timestampReceived = Integer.parseInt(st.nextToken());
				String message_id = st.nextToken();
				//String requestedResource = st.nextToken();

				System.out.print("+ Mensagem recebida: " + messageType);
				System.out.print(". id: " + message_id);
				System.out.print(". remetente: " + sender);
				System.out.print(". timestamp: " + timestampReceived);
				//System.out.print(". recurso: " + requestedResource);
				System.out.println(".");

				/* Atualiza o timestamp pegando o máximo. */
				int actualTimestamp = timestamp.intValue();
				timestamp.set((actualTimestamp > timestampReceived) ? actualTimestamp + 1 : timestampReceived + 1);

			
				if ( messageType.equals(REQUEST) ) {
					/* REQUEST recebido, verifique o estado do processo sobre o recurso e responda ao remetente. */

					System.out.println("\t\tProcesso C sobre o recurso : " + resourceState);

					switch ( resourceState ) {
						case NO_WANT:
							/* Apenas envie OK para o remetente. */
							System.out.println("\t\tProcesso C vai mandar OK para " + sender);
							startClientSide("OK@", sender);
							break;

						case WANT:
							/* Verifique quem tem o menor timestamp. Quem tiver o menor ganha o recurso. */
							if ( requestTimestamp < timestampReceived ) {
								/* Mensagem REQUEST do Processo C possui timestamp menor, logo ele tem direito ao recurso. */
								System.out.println("\t\tProcesso C vai mandar WAIT para " + sender + " porque " + requestTimestamp + " < " + timestampReceived);
								startClientSide("WAIT@", sender);

								/* Adiciona o remetente à fila de processos. Depois que o Processo C usar o recurso, ele mandará REPLY para todos os processos na fila. */
								
									filaRecurso.add(sender);
								
								System.out.println("\t\t" + sender + " foi adicionado à fila.");
							}
							else {
								System.out.println("\t\tProcesso C vai mandar OK para " + sender + " porque " + requestTimestamp + " >= " + timestampReceived);
								startClientSide("OK@", sender);
							}
							break;

						case ACCESSING_RESOURCE:
							/* Mande o remetente esperar. */
							System.out.println("\t\tProcesso C vai mandar WAIT para " + sender);
							/* Adiciona o remetente à fila de processos. Depois que o Processo C usar o recurso, ele mandará REPLY para todos os processos na fila. */
							
								filaRecurso.add(sender);
							
							System.out.println("\t\t" + sender + " foi adicionado à fila.");
							startClientSide("WAIT@", sender);
							break;
					}

				}
				else {
					/* REPLY recebido. */
					/* Se for um OK, incremente a quantidade de OKs recebidos para o recurso no referido índice. */
					/* Se for um WAIT, não faça nada. O remetente vai enviar um OK quando terminar de usar o recurso. */
					if ( message_id.equals("OK") ) {
						okFromProcesses++;
					}
				}
			}
		}
		catch ( IOException e ) {
			e.printStackTrace();
		}

	}

}
