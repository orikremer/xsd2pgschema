/*
    xsd2pgschema - Database replication tool based on XML Schema
    Copyright 2018 Masashi Yokochi

    https://sourceforge.net/projects/xsd2pgschema/

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
 */

import net.sf.xsd2pgschema.*;
import net.sf.xsd2pgschema.option.*;
import net.sf.xsd2pgschema.serverutil.*;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.BindException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.util.ArrayList;
import java.util.List;

import org.nustaq.serialization.FSTConfiguration;

/**
 * PgSchema server control.
 *
 * @author yokochi
 */
public class pgschemaserv {

	/**
	 * The main method.
	 *
	 * @param args the arguments
	 */
	public static void main(String[] args) {

		/** The FST configuration. */
		FSTConfiguration fst_conf = FSTConfiguration.createDefaultConfiguration();

		fst_conf.registerClass(PgSchemaServerQuery.class,PgSchemaServerReply.class); // FST optimization

		/** The PostgreSQL data model option. */
		PgSchemaOption option = new PgSchemaOption(true);

		/** Whether to report PgSchema server status. */
		boolean status = false;

		/** Whether to start PgSchema server. */
		boolean start = true;

		/** The PgSchema server name. */
		final String server_name = "PgSchema Server";

		/** The red color code. */
		final String red_color = (char)27 + "[31m";

		/** The green color code. */
		final String green_color = (char)27 + "[32m";

		/** The yellow color code. */
		final String yellow_color = (char)27 + "[33m";

		/** The blue color code. */
		final String blue_color = (char)27 + "[34m";

		/** The default color code. */
		final String default_color = (char)27 + "[39m";

		/** The header of PgSchema server information. */
		final String server_info_header = "[" + blue_color + server_name + default_color + "] ";

		/** The header of PgSchema server start information. */
		final String server_start_info_header = server_info_header + green_color + "START" + default_color + " ---";

		/** The footer of PgSchema server information. */
		final String server_info_footer = default_color + "\n";

		for (int i = 0; i < args.length; i++) {

			if (args[i].equals("--host") && i + 1 < args.length)
				option.pg_schema_server_host = args[++i];

			else if (args[i].equals("--port") && i + 1 < args.length)
				option.pg_schema_server_port = Integer.valueOf(args[++i]);

			else if (args[i].equals("--lifetime") && i + 1 < args.length)
				option.pg_schema_server_lifetime = Integer.valueOf(args[++i]) * 1000L;

			else if (args[i].equals("--start")) {
				start = true;
				status = false;
			}

			else if (args[i].equals("--status")) {
				status = true;
				start = false;
			}

			else if (args[i].equals("--stop"))
				status = start = false;

			else {
				System.err.println("Illegal option: " + args[i] + ".");
				showUsage();
			}

		}

		if (option.pg_schema_server_lifetime <= 0) {
			System.err.println("Lifetime shuld be positive.");
			showUsage();
		}

		// send status query

		if (status) {

			try {

				try (Socket socket = new Socket(InetAddress.getByName(option.pg_schema_server_host), option.pg_schema_server_port)) {

					DataOutputStream out = new DataOutputStream(socket.getOutputStream());
					DataInputStream in = new DataInputStream(socket.getInputStream());

					PgSchemaUtil.writeObjectToStream(fst_conf, out, new PgSchemaServerQuery(PgSchemaServerQueryType.STATUS));

					PgSchemaServerReply reply = (PgSchemaServerReply) PgSchemaUtil.readObjectFromStream(fst_conf, in);

					System.out.print(reply.message);
					/*
					in.close();
					out.close();
					 */
				}

			} catch (IOException | ClassNotFoundException e) {
				e.printStackTrace();
				System.exit(1);
			}

		}

		// start server

		else if (start) {

			try {

				ServerSocket server_socket = new ServerSocket(option.pg_schema_server_port);

				System.out.println("\n" + server_start_info_header + " port number: " + option.pg_schema_server_port + "\n" + server_start_info_header + " lifetime   : " + (option.pg_schema_server_lifetime / 1000L) + " sec");

				List<PgSchemaServerImpl> list = new ArrayList<PgSchemaServerImpl>();

				while (!server_socket.isClosed()) {

					Thread thrd = new Thread(new PgSchemaServerThrd(option, fst_conf, server_socket, server_socket.accept(), list));

					thrd.setPriority(Thread.MAX_PRIORITY);
					thrd.start();

				}

			} catch (BindException e) {
				System.out.print("\n" + server_info_header + yellow_color + "ALREADY RUNNING" + default_color + " --- port number: " + option.pg_schema_server_port + "\n");
				System.exit(0);	
			} catch (SocketException e) {
				System.out.print("\n" + server_info_header + red_color + "STOP" + server_info_footer);
				System.exit(0);	
			} catch (IOException e) {
				e.printStackTrace();
				System.exit(1);
			}

		}

		// send stop query

		else {

			try (Socket socket = new Socket(InetAddress.getByName(option.pg_schema_server_host), option.pg_schema_server_port)) {

				DataOutputStream out = new DataOutputStream(socket.getOutputStream());

				PgSchemaUtil.writeObjectToStream(fst_conf, out, new PgSchemaServerQuery(PgSchemaServerQueryType.STOP));
				/*
				out.close();
				 */
			} catch (IOException e) {
				e.printStackTrace();
				System.exit(1);
			}

		}

	}

	/**
	 * Show usage.
	 */
	private static void showUsage() {

		System.err.println("pgschemaserv: PgSchema server control");
		System.err.println("Usage:  --port PG_SCHEMA_SERV_PORT_NUMBER (default=\"" + PgSchemaUtil.pg_schema_server_port + "\")");
		System.err.println("Option: --host PG_SCHEMA_SERV_HOST_NAME (default=\"" + PgSchemaUtil.pg_schema_server_host + "\")");
		System.err.println("        --lifetime LIFETIME_SECOND (default=\"" + (PgSchemaUtil.pg_schema_server_lifetime / 1000L) + "\")");
		System.err.println("        --start (start PgSchema server, default)");
		System.err.println("        --status (report PgSchema server status)");
		System.err.println("        --stop (stop PgSchema server)");
		System.exit(1);

	}

}
