#include "./concurrentqueue.h"
#include "./db.h"
#include "rocksdb/options.h"

using namespace rocksdb;


struct TransferSstFilesArgs {
	DB* db;
	Options options;
	std::vector<std::string> dbname;
	std::vector<SstFileData> file_datas;
	int target_level;
	int number_of_files;
	int number_of_threads;
	std::vector<int> sst_fd;
	std::string id;

	TransferSstFilesArgs(DB* db_, 
						 const Options& options_, 
						 const std::vector<std::string>& dbname_, 
						 const std::vector<SstFileData>& file_datas_, 
						 const int& target_level_, 
						 const int& number_of_files_, 
						 const int& number_of_threads_, 
						 const std::vector<int>& sst_fd_, 
						 const std::string& id_) {
		db = db_;
		options = options_;
		dbname = dbname_;
		file_datas = file_datas_;
		target_level = target_level_;
		number_of_files = number_of_files_;
		number_of_threads = number_of_threads_;
		sst_fd = sst_fd_;
		id = id_;
	}
};



class SstFile {
private:
	// moodycamel::ConcurrentQueue<std::string> file_queue;
public:
	void RecvKeys(const int& fd, 
				  const uint64_t& number_of_entries, 
				  moodycamel::ConcurrentQueue<std::string>& key_queue);
	void RecvValues(const int& fd, 
					const uint64_t& number_of_entries, 
					moodycamel::ConcurrentQueue<std::string>& value_queue);
	void ImportKVPairs(const Options& options, 
					   const uint64_t& number_of_entries, 
					   const std::string& file_path, 
					   moodycamel::ConcurrentQueue<std::string>& key_queue, 
					   moodycamel::ConcurrentQueue<std::string>& value_queue);
	// void IngestSstFiles(DB* db, const int& file_level);
};



// void TransferSstFiles(TransferSstFilesArgs args, bool& share);
void TransferSstFiles(TransferSstFilesArgs args);
