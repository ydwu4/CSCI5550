extern"C"{
#include <log.h>
}

#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include <io_manager.hpp>


void my_free (void *data, void *hint) {
	free (data);
}

int IOManager::init_sockets(int num_workers, int starting_port) {	
	fprintf(stderr, "master has %d workers\n", num_workers);	

	this->context = zmq_ctx_new();

	for (int i = 0; i < num_workers; i++) {
		std::string address = "tcp://127.0.0.1:" + std::to_string(starting_port+i);
		fprintf(stderr, "master bind a socket to address:%s\n", address.c_str());
		void *req = zmq_socket(context, ZMQ_REQ);
		int res = zmq_bind(req, address.c_str());
		if (res != 0) {
			fprintf(stderr, "master fails to bind a socket to address:%s\n", address.c_str());
		}
		this->sockets.push_back(req);
	}
	fprintf(stderr, "master initialize sockets successfully\n");
}


// send a char '1' to all the workers and wait for reply 
int IOManager::init() {
	fprintf(stderr, "master starts to send init msg\n");
	for(int i = 0; i < this->sockets.size(); i++) {
		char *buf = (char*)malloc(sizeof(char)*1);
		if (buf == NULL) {
			fprintf(stderr, "malloc buf fails\n");
			exit(-1);
		}
		fprintf(stderr, "malloc buf succeeds\n");
		buf[0] = '1';
		fprintf(stderr, "fill buf with '1'\n");
		zmq_msg_t msg;
		zmq_msg_init_data(&msg, buf, 1, my_free, NULL);
		int sent_size = zmq_msg_send(&msg, sockets[i], 0);
		if (sent_size == -1) {
			fprintf(stderr, "master send init fails with %s\n", strerror(errno));
		}
		assert(sent_size == 1);
		fprintf(stderr, "master sent init msg to socket:%d\n", i);
	}

	for(int i = 0; i < this->sockets.size(); i++) {
		zmq_msg_t msg;
		int rc = zmq_msg_init(&msg);
		if (rc != 0) {
			fprintf(stderr, "master receive init reply init fails with %s\n", strerror(errno));
		}
		int recv_size = zmq_msg_recv(&msg, sockets[i], 0);
		if (recv_size == -1) {
			fprintf(stderr, "master receive init reply fails with %s\n", strerror(errno));
		}

		char *data = (char*) zmq_msg_data(&msg);
		if (strcmp(data, "ok") == 0){
			fprintf(stderr, "master receive `ok`\n");
		}
		else {
			fprintf(stderr, "master did not receive `ok` but `%s`\n", data);
		}
	}

	fprintf(stderr, "all sockets has already connected to master!\n");
}
