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

package net.sf.xsd2pgschema.serverutil;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.util.Calendar;
import java.util.List;
import java.util.Optional;

import javax.xml.bind.DatatypeConverter;

import org.nustaq.serialization.FSTConfiguration;

import net.sf.xsd2pgschema.PgSchemaUtil;
import net.sf.xsd2pgschema.option.PgSchemaOption;

/**
 * Thread function for PgSchema server.
 *
 * @author yokochi
 */
public class PgSchemaServerThrd implements Runnable {

	/** The FST configuration. */
	private FSTConfiguration fst_conf;

	/** The PgSchema server socket. */
	private ServerSocket server_socket;

	/** The PgSchema socket. */
	private Socket socket;

	/** The list of serialized PostgreSQL data models. */
	private List<PgSchemaServerImpl> list;

	/** The default lifetime of unused PostgreSQL data model on PgSchema server in milliseconds. */
	private long pg_schema_server_lifetime;

	/** The reply object of PgSchema server. */
	private PgSchemaServerReply reply = new PgSchemaServerReply();

	/** Whether receive STOP query. */
	private boolean stop = false;

	/** The PgSchema server name. */
	final String server_name = "PgSchema Server";

	/** The red color code. */
	final String red_color = (char)27 + "[31m";

	/** The green color code. */
	final String green_color = (char)27 + "[32m";

	/** The blue color code. */
	final String blue_color = (char)27 + "[34m";

	/** The default color code. */
	final String default_color = (char)27 + "[39m";

	/** The header of PgSchema server information. */
	final String server_info_header = "[" + blue_color + server_name + default_color + "] ";

	/** The header of PgSchema server status information. */
	final String server_status_info_header = server_info_header + blue_color + "STATUS" + default_color + " ---";

	/** The footer of PgSchema server information. */
	final String server_info_footer = default_color + "\n";

	/**
	 * Instance of PgSchemaServerThrd.
	 *
	 * @param option PostgreSQL data model option
	 * @param fst_conf FST configuration
	 * @param server_socket server socket
	 * @param socket socket
	 * @param list list of serialized PostgreSQL data models
	 */
	public PgSchemaServerThrd(final PgSchemaOption option, final FSTConfiguration fst_conf, final ServerSocket server_socket, final Socket socket, final List<PgSchemaServerImpl> list) {

		this.fst_conf = fst_conf;
		this.server_socket = server_socket;
		this.socket = socket;

		list.removeIf(arg -> arg.isObsolete(System.currentTimeMillis(), pg_schema_server_lifetime = option.pg_schema_server_lifetime));

		this.list = list;

	}

	/* (non-Javadoc)
	 * @see java.lang.Runnable#run()
	 */
	@Override
	public void run() {

		try {

			DataOutputStream out = new DataOutputStream(socket.getOutputStream());
			DataInputStream in = new DataInputStream(socket.getInputStream());

			PgSchemaServerQuery query = (PgSchemaServerQuery) PgSchemaUtil.readObjectFromStream(fst_conf, in);

			switch (query.type) {
			case ADD:
				PgSchemaServerImpl new_item = new PgSchemaServerImpl(query);
				list.add(new_item);

				reply.message = server_info_header + blue_color + "ADD" + server_info_footer;
				break;
			case MATCH:
				Optional<PgSchemaServerImpl> match_opt = list.stream().filter(arg -> arg.option.equals(query.option)).findFirst();

				if (match_opt.isPresent())
					reply.message = server_info_header + green_color + "MATCH" + server_info_footer;
				else
					reply.message = server_info_header + red_color + "MATCH NOTHING" + server_info_footer;
				break;
			case GET:
				Optional<PgSchemaServerImpl> get_opt = list.stream().filter(arg -> arg.option.equals(query.option)).findFirst();

				if (get_opt.isPresent()) {

					PgSchemaServerImpl get_item = get_opt.get();

					get_item.touch();

					reply.message = server_info_header + green_color + "GET" + server_info_footer;
					reply.schema_bytes = get_item.schema_bytes;

				} else
					reply.message = server_info_header + red_color + "GET NOTHING" + server_info_footer;
				break;
			case PING:
				reply.message = server_info_header + blue_color + "PING OK" + server_info_footer;
				break;
			case STATUS:
				StringBuilder sb = new StringBuilder();

				sb.append(server_status_info_header + " count of data models: " + list.size() + "\n");

				list.forEach(arg -> {

					sb.append(server_status_info_header + "-----------------------------------------------------------\n");
					sb.append(server_status_info_header + " default schema location: " + arg.option.root_schema_location + "\n");
					sb.append(server_status_info_header + " original caller class  : " + arg.original_caller + "\n");
					sb.append(server_status_info_header + " length of data model   : " + arg.schema_bytes.length + "\n");
					sb.append(server_status_info_header + " hash code of data model: " + arg.option.hashCode() + "\n");

					Calendar dt_cal = Calendar.getInstance();
					dt_cal.setTimeInMillis(arg.last_access_time_millis);

					sb.append(server_status_info_header + " last accessed time     : " + DatatypeConverter.printDateTime(dt_cal) + "\n");

					dt_cal.setTimeInMillis(arg.last_access_time_millis + pg_schema_server_lifetime);

					sb.append(server_status_info_header + " expired time           : " + DatatypeConverter.printDateTime(dt_cal) + "\n");

				});

				reply.message = sb.toString();

				sb.setLength(0);
				break;
			case STOP:
				list.clear();

				stop = true;

				break;
			}

			if (!query.type.equals(PgSchemaServerQueryType.STOP))
				PgSchemaUtil.writeObjectToStream(fst_conf, out, reply);
			/*
			in.close();
			out.close();
			 */
		} catch (SocketException e) {
			System.out.print("\n" + server_info_header + red_color + "STOP" + server_info_footer);
			System.exit(0);
		} catch (ClassNotFoundException | IOException e) {
			e.printStackTrace();
		} finally {

			try {

				socket.close();

				if (stop)
					server_socket.close();

			} catch (IOException e) {
				e.printStackTrace();
			}

		}

	}

}
