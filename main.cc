// Destination
#include <iostream>
#include <thread>
#include <unistd.h>
#include <sys/types.h>
#include <sys/socket.h>
#include "./sst.h"
#include "./socket.h"
#include "./debug.h"

using namespace rocksdb;


RunTime mig_time;


static void StartInstance(const std::string& id, const int& rate_of_workload, const int& number_of_threads) {
	std::cout << "Destination Instance is started." << std::endl;

/*-------------------------------------- Phase 1 --------------------------------------*/


    // ソケットをオープン
	SocketFD socket_fd;
	OpenSocket(id, number_of_threads, socket_fd);


	// DBのパスを指定
	const std::string db_path = "/mig_inst" + id;
#ifdef ONLY_PMEM
	const std::string pmem_db_path = pmem_db_dir + db_path;
	const std::vector<std::string> dbname = {pmem_db_path};
#elif ONLY_DISK
	const std::string disk_db_path = disk_db_dir + db_path;
	const std::vector<std::string> dbname = {disk_db_path};
#else
	const std::string pmem_db_path = pmem_db_dir + db_path;
	const std::string disk_db_path = disk_db_dir + db_path;
	const std::vector<std::string> dbname = {pmem_db_path, disk_db_path};
#endif


/*-------------------------------------- Phase 1 --------------------------------------*/


	// SSTableメタデータを受信
	std::vector<SstFileData> file_datas;
	RecvSstFileData(socket_fd.main_fd, file_datas);

	// Optionsを受信
	SrcOptions src_options;
	RecvOptions(socket_fd.main_fd, src_options);

	Options options;
	options.IncreaseParallelism();
	options.create_if_missing = true;
	// options.write_buffer_size = src_options.write_buffer_size;
#ifdef MIXING
	options.db_paths = {{dbname[0], PMEMSIZE}, {dbname[1], DISKSIZE}};
#endif

	// DBを作成
	DB* db = CreateDB(options, dbname[0]);
	std::cout << "dbname: " << dbname[0] << std::endl;

	SendFlag(socket_fd.main_fd, 1);


	// オートコンパクションをオフにする
	// Status status;
	// status = db->SetOptions({{"disable_auto_compactions", "true"},});
	// AssertStatus(status, "SetOptions");


/*-------------------------------------- Phase 2 --------------------------------------*/

	
	mig_time.start = std::chrono::system_clock::now();
	
	// SSTables転送スレッドに渡す引数を初期化
#ifdef RELOCATION
	int target_level = TargetLevel(options.db_paths[0].target_size, 
								   options.max_bytes_for_level_base, 
								   options.target_file_size_base, 
								   options.max_bytes_for_level_multiplier, 
								   options.target_file_size_multiplier);
#else
	int target_level = -1;
#endif

	// bool share = false;

	TransferSstFilesArgs args(db, 
							  options, 
							  dbname, 
							  file_datas, 
							  target_level, 
							  src_options.number_of_files, 
							  number_of_threads, 
							  socket_fd.sst_fd, 
							  id);

	// SSTables転送スレッドを実行
	// std::thread TransferSstFilesThread([&]{ TransferSstFiles(args, std::ref(share)); });
	std::thread TransferSstFilesThread([&]{ TransferSstFiles(args); });
	TransferSstFilesThread.join();


	mig_time.end = std::chrono::system_clock::now();
    mig_time.time = ReturnRunTime(mig_time.start, mig_time.end);
    std::cout << "Migration_Time: " << mig_time.time / 1000000 << " seconds" << std::endl;


/*-------------------------------------- Phase 3 --------------------------------------*/


	// オートコンパクションをオンにする
	// status = db->SetOptions({{"disable_auto_compactions", "false"},});
	// AssertStatus(status, "SetOptions");


	// ソケットをクローズ
	CloseSocket(number_of_threads, socket_fd);


	delete db;
}



int main(int argc, char* argv[]) {
	if (argc != 4) {
		std::cerr << "argument error" << std::endl;
		return -1;
	}

	const std::string id = argv[1];
	int rate_of_workload = atoi(argv[2]);
	int number_of_threads = atoi(argv[3]);

	StartInstance(id, rate_of_workload, number_of_threads);

	return 0;
}
