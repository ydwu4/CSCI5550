#pragma once
#include <zmq.h>

#include <vector>
#include <string>


class IOManager {
	public:
		static IOManager &get_instance() {
			static IOManager io_manager;
			return io_manager;
		}

		int init_sockets(int num_workers, int starting_port);

		int init();

		int get_attr();

		int send_small_file();

		int send_large_file();

		int write_small_file();

		int write_large_file();

	private:
		IOManager() {}

		~IOManager() {}

		void *context;
		std::vector<void*> sockets;
};
