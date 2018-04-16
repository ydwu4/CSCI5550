#pragma once

extern"C"{
#include <zmq.h>
}

#include <thread>
#include <iostream>
#include <vector>
#include <string>
#include <utility>


#include <assert.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <errno.h>
#include <string.h>

using namespace std;

	class Worker {
	public:
		static Worker& get_instance() {
			static Worker worker;
			return worker;
		}

		void set_master_addr(std::string addr) {
			this->master_addr = addr;
		}

		void set_master_port(int port) {
			this->master_port = port;
		}

		void init_socket() {
			std::string master_point = "tcp://" + this->master_addr + ":" + std::to_string(this->master_port);	
			fprintf(stderr, "master address:%s\n", master_point.c_str());
			this->context = zmq_ctx_new();
			this->socket = zmq_socket(context, ZMQ_REP);
			int res = zmq_connect(this->socket, master_point.c_str());
			if (res != 0) {
				cout << "worker: connect to " << master_point << " fails with error:" << strerror(errno) << endl;				
			}
		}
		
		// this is the main function
		void serve() {
			fprintf(stderr, "start to serve\n");

			int rc = 0;
			zmq_msg_t msg;
			rc = zmq_msg_init(&msg);
			if (rc != 0) {
				cout << "worker fails to init zmq msg" << endl;
			}

			this->running = true;
			while(this->running) {
				// this is the first part of the message
				// it shows what kind of operation the master wants to do
				int size = zmq_msg_recv(&msg, this->socket, 0);
				if (size < 0) {
					cout << "worker fails to recieve message about operations" << endl;
				}
				
				// there are 5 kinds of msg
				// "1" is for initialization
				// "2" is for small file read
				// "3" is for large file read
				// "4" is for small file write
				// "5" is for large file write
				// "6" is for get metadata (TODO)
				assert(size == 1);
				char *data = (char*) zmq_msg_data(&msg);
				assert (data != NULL);
				
				switch(data[0]) {
				case '1':
					init(&msg);
				case '2':
					small_file_read(&msg);
				case '3':
					large_file_read(&msg);
				case '4':
					small_file_write(&msg);
				case '5':
					large_file_write(&msg);
				case '6':
					get_metadata(&msg);
				default:
					cout << "error unknown message type" << endl;
				}	
			}
			cout << "worker exit main loop and try to close msg" << endl;
			rc = zmq_msg_close(&msg);	
			if (rc != 0) {
				cout << "worker fails to close the message but we will exit anyway" << endl;
			}
			assert(rc == 0);
		}

		void init(zmq_msg_t *msg) {
			cout << "work replys to init msg" << endl;
			// zmq will take the ownership of the data buffer
			// so we malloc a buffer and pass to zmq_msg_init_data
			char *data = (char*)malloc(sizeof(char) * 3);
			strcpy(data, "ok");
			zmq_msg_t reply_msg;
			zmq_msg_init_data(&reply_msg, data, 3, NULL, NULL); 
			int send_size = zmq_msg_send(&reply_msg, this->socket, 0);
			if (send_size == -1) {
				cout << "worker reply ok fails with error" << strerror(errno) << endl;
			}	
			else if (send_size < 3) {
				cout << "worker only sends " << send_size << " bytes but should send 2 bytes" << endl;
			}
		}

		void small_file_read(zmq_msg_t *msg) {

		}

		void large_file_read(zmq_msg_t *msg) {

		}

		void small_file_write(zmq_msg_t *msg) {

		}

		void large_file_write(zmq_msg_t *msg) {

		}

		void get_metadata(zmq_msg_t *msg) {

		}

		bool is_running() {
			return this->running;
		}
	
	protected:
		Worker() {
		
		}


	private:
		bool running = false;

		std::string master_addr;
		int master_port;

		void *context;
		void *socket;
	};
