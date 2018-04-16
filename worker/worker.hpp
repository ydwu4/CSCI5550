#pragma once

extern"C"{
#include <zmq.h>
}

#include <thread>
#include <iostream>
#include <fstream>
#include <sstream>
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

		// 2, 
		// filename 
		// offset len 
		void small_file_read(zmq_msg_t *msg) {
			vector<string> vec;
			recv_more(vec, msg);

			std::string filename = vec[1];
			vector<string> meta_vec = parse_to_vector(vec[2]);
			int offset = stoi(meta_vec[0]);
			int len = stoi(meta_vec[1]) * len;

			char* buffer = (char*)malloc(sizeof(char) *len);
			local_read(filename, len, offset, buffer);
			send_to_master(buffer, len);
			free(buffer);
		}

		// 4, 
		// filename 
		// len offset 
		// data
		void small_file_write(zmq_msg_t *msg) {
			vector<string> vec;
			recv_more(vec, msg);

			std::string filename = vec[1];

			vector<string> meta_vec = parse_to_vector(vec[2]);
			int len = stoi(meta_vec[0]);
			int offset = stoi(meta_vec[1]) * len;
			const char* buf = vec[3].c_str();

			local_write(filename, len, offset, buf);

			int ok_len = 2;
			char * ok_msg = (char*)malloc(sizeof(char) * ok_len);
			strncpy(ok_msg, "ok", ok_len);
			send_to_master(ok_msg, ok_len);
			free(ok_msg);
		}

		// 3, filename len offset
		// [filename len offset]*

		void large_file_read(zmq_msg_t *msg) {
			int filename_idx = 0;
			int len_idx = 1;
			int offset_idx = 2;
          	vector<string> vec;
			recv_more(vec, msg);


			// parse meta
			vector<vector<string>> meta_vec;
			for (int i=1; i<vec.size(); i++) {
				meta_vec.push_back(parse_to_vector(vec[i]));
			}

			// compute total_len
			int total_len = stoi(meta_vec[0][len_idx]);
			for (int i=1; i<meta_vec.size(); i++) {
				assert(stoi(meta_vec[i-1][len_idx]) + stoi(meta_vec[i-1][offset_idx]) == stoi(meta_vec[i][offset_idx]));
				total_len += stoi(meta_vec[i][len_idx]);
			}

			int initial_offset = stoi(meta_vec[0][offset_idx]);
			string filename = meta_vec[0][filename_idx];

			char* buffer = (char*)malloc(sizeof(char) * total_len);
			local_read(filename, total_len, initial_offset, buffer);
			send_to_master(buffer, total_len);
			free(buffer);
		}

		// assume message is the form of
		// 6, 
		// filename len1 offset1,
		// data1
		// [string:filename int:len2 int:offset2 string:data2]
		// [....]
		void large_file_write(zmq_msg_t *msg) {
			int name_idx = 0;
			int len_idx = 1;
			int offset_idx = 2;
			vector<string> vec;
			recv_more(vec, msg);
			// assert for the continuity of data blocks

			// parse meta data
			vector<vector<string>> meta_vec;
			for(int i = 1; i<vec.size(); i+=2) {
				meta_vec.push_back(parse_to_vector(vec[i]));
			}
			// get data string ptr
			vector<const char*> buf_ptr;
			for (int i=2; i<vec.size(); i+=2) {
				buf_ptr.push_back(vec[i].c_str());
			}
			assert(buf_ptr.size() == meta_vec.size());

			// get total len
			int total_len = stoi(meta_vec[0][len_idx]);
			for (int i=1; i<meta_vec.size(); i++) {
				assert(stoi(meta_vec[i-1][len_idx]) + stoi(meta_vec[i-1][offset_idx]) == stoi(meta_vec[i][offset_idx]));
				total_len += stoi(meta_vec[i][len_idx]);
			}

			char* data = (char*) malloc(sizeof(char) * total_len);
			// copy to new chunk
			int offset = 0;
			for (int i=0; i<buf_ptr.size(); i++) {
				int len = stoi(meta_vec[i][len_idx]);
				memmove(data+offset, buf_ptr[i], len);
				offset += len;
			}
			assert(offset == total_len);

			string filename = meta_vec[0][name_idx];
			int initial_offset = stoi(meta_vec[0][offset_idx]); 
			local_write(filename, total_len, initial_offset, data);


			int ok_len = 2;
			char * ok_msg = (char*)malloc(sizeof(char) * ok_len);
			strncpy(ok_msg, "ok", ok_len);
			send_to_master(ok_msg, ok_len);
			free(data);
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
	private:	
		vector<string> parse_to_vector(string str) {
			istringstream iss(str);
			vector<string> vec;
			string tmp;
			while (getline( iss, tmp, ' ' )) {
			   	vec.push_back(tmp);
			}
			cout<<"parsed: ";
			for(auto& a : vec) {
			    cout<< a <<",";
			}
			cout<<endl;

			return vec;			
		}
		void local_read(string filename, int len, int offset, char* buf) {
			ifstream in(filename, ifstream::in | ifstream::binary);
			if (in.is_open()) {
				in.seekg(offset);
				in.read(buf, len);
				in.close();				
			} else {
				cout<< "Enable to open file " <<filename << endl;
			}

		}
		void local_write(string filename, int len, int offset, const char* buf) {
			ofstream out(filename, ifstream::out | ifstream::binary);
			if (out.is_open()) {
				out.seekp(offset);
				out.write(buf, len);
				out.close();
			} else {
				cout<< "Enable to open file " << filename <<endl;
			}
		}

		void send_to_master(char* data, int len) {
			zmq_msg_t msg;
			zmq_msg_init_data(&msg, data, len, NULL, NULL);
			int sent_size = zmq_msg_send(&msg, this->socket, 0);
			assert(sent_size == len);
		}

		void recv_more(vector<string>& buf, zmq_msg_t* msg) {
			buf.clear();

			char *data = (char*) zmq_msg_data(msg);
			string part(data);
			buf.push_back(part);
			while(zmq_msg_more(msg)) {
				int size = zmq_msg_recv(msg, this->socket, 0);
				if (size < 0) {
					cout << "recv_more: worker fails to recieve message about operations" << endl;
				}
				data = (char*) zmq_msg_data(msg);
				assert (data != NULL);
				string p(data);
				buf.push_back(p);
			}
		}
	};
