#include <fuse.h>
#include <string.h>

extern"C"{
#include <log.h>
}
#include <worker.hpp>

static struct options {
	int index;
	int debug;
	int starting_port;
	int show_help;
	const char* master_addr;
	const char* log_filename;
} options;

#define OPTION(t, p) \
	{ t, offsetof(struct options, p), 1 }

static const struct fuse_opt option_spec[] = {
	OPTION("-d", debug),
	OPTION("-h", show_help),
	OPTION("--debug", debug),
	OPTION("--help", show_help),
	OPTION("--master-address=%s", master_addr),
	OPTION("--starting-port=%d", starting_port),
	OPTION("--log-filename=%s", log_filename),
	OPTION("--index=%d", index),
	FUSE_OPT_END
};

static void show_help(const char *progname) {
	printf("usage: %s [options]\n\n", progname);
	printf("Worker specific options:\n"
		   " --index=<d>               The index of worker  [Required]\n"
		   " --starting-port=<d>       The starting port of master [Defualt] 17000\n"
		   " --debug, -d               Enable debug         [Default] Enable debug\n"
		   " --help, -h                Show help            [Default] Show help\n"
		   " --master-addr=<s>         Set master addr      [Default] 127.0.0.1\n"
		   " --log_filename=<s>        Log filename         [Defualt] worker`index`.log\n"
		   "\n");

}



int main(int argc, char *argv[])
{
	
	struct fuse_args args = FUSE_ARGS_INIT(argc, argv);

	options.debug = 0;
	options.show_help = 0;
	options.index = -1;
	options.master_addr = strdup("127.0.0.1");
	options.starting_port = 17000;
		
	/* Parse options */
	if (fuse_opt_parse(&args, &options, option_spec, NULL) == -1) {
		return 1;
	}

	if (options.show_help) {
		show_help(argv[0]);
		return 0;
	}

	if (options.index == -1) {
		show_help(argv[0]);
		return 0;
	}



	char log_filename[50];
	sprintf(log_filename, "worker%d.log", options.index);
	fprintf(stderr, "log filename:%s\n", log_filename);
	log_open(log_filename);


	fprintf(stderr, "master_addr:%s\n", options.master_addr);
	fprintf(stderr, "starting_port = %d\n", options.starting_port);
	Worker &worker = Worker::get_instance();
	fprintf(stderr, "worker index:%u\n", options.index);	
	worker.set_master_addr(options.master_addr);
	worker.set_master_port(options.starting_port + options.index);
	worker.init_socket();
	// stuck in the loop
	worker.serve();

	return 0;
}
