#include "ceph_test_rados_io_sequence.h"

#include <iostream>
#include <vector>

#include <boost/asio/io_context.hpp>

#include "include/random.h"

#include "librados/librados_asio.h"
#include "common/ceph_argparse.h"
#include "include/interval_set.h"
#include "global/global_init.h"
#include "global/global_context.h"
#include "common/Thread.h"
#include "common/debug.h"
#include "common/dout.h"
#include "common/split.h"

#include "common/io_exerciser/DataGenerator.h"
#include "common/io_exerciser/Model.h"
#include "common/io_exerciser/ObjectModel.h"
#include "common/io_exerciser/RadosIo.h"
#include "common/io_exerciser/IoOp.h"
#include "common/io_exerciser/IoSequence.h"

#define dout_subsys ceph_subsys_rados
#define dout_context g_ceph_context

template <typename T, int N, const std::array<T, N>& Ts>
ceph::io_sequence::tester::ProgramOptionSelector<T, N, Ts>
  ::ProgramOptionSelector(ceph::util::random_number_generator<int>& rng,
                          po::variables_map vm,
                          const std::string& option_name,
                          bool set_forced,
                          bool select_first) 
  : rng(rng),
    // choices(choices),
    option_name(option_name) {
  if (set_forced && vm.count(option_name)) {
    force_value = vm[option_name].as<T>();
  }
  if (select_first) {
    ceph_assert(choices.size() > 0);
    first_value = choices[0];
  }
}

template <typename T, int N, const std::array<T, N>& Ts>
bool ceph::io_sequence::tester::ProgramOptionSelector<T, N, Ts>::isForced()
{
  return force_value.has_value();
}

template <typename T, int N, const std::array<T, N>& Ts>
const T ceph::io_sequence::tester::ProgramOptionSelector<T, N, Ts>::choose()
{
  if (force_value.has_value()) {
    return *force_value;
  } else if (first_value.has_value()) {
    return *std::exchange(first_value, std::nullopt);
  } else {
    return choices[rng(N-1)];
  }
}



ceph::io_sequence::tester::SelectObjectSize::SelectObjectSize(
    ceph::util::random_number_generator<int>& rng,
    po::variables_map vm)
  : ProgramOptionSelector(rng, vm, "objectsize", true, true)
{
}



ceph::io_sequence::tester::SelectBlockSize::SelectBlockSize(
    ceph::util::random_number_generator<int>& rng,
    po::variables_map vm)
  : ProgramOptionSelector(rng, vm, "blocksize", true, true)
{
}



ceph::io_sequence::tester::SelectNumThreads::SelectNumThreads(
    ceph::util::random_number_generator<int>& rng,
    po::variables_map vm) 
  : ProgramOptionSelector(rng, vm, "threads", true, true)
{
}



ceph::io_sequence::tester::SelectSeqRange::SelectSeqRange(
    ceph::util::random_number_generator<int>& rng,
    po::variables_map vm) 
  : ProgramOptionSelector(rng, vm, "sequence", false, false)
{
  if (vm.count(option_name)) {
    ceph::io_exerciser::Sequence s = 
      static_cast<ceph::io_exerciser::Sequence>(vm["sequence"].as<int>());
    if (s < ceph::io_exerciser::Sequence::SEQUENCE_BEGIN || 
        s >= ceph::io_exerciser::Sequence::SEQUENCE_END) {
      dout(0) << "Sequence argument out of range" << dendl;
      throw po::validation_error(po::validation_error::invalid_option_value);
    }
    ceph::io_exerciser::Sequence e = s;
    force_value = std::make_optional<std::pair<ceph::io_exerciser::Sequence,
                                               ceph::io_exerciser::Sequence>>(
                    std::make_pair(s, ++e));
  }
}

const std::pair<ceph::io_exerciser::Sequence,ceph::io_exerciser::Sequence>
  ceph::io_sequence::tester::SelectSeqRange::choose() {
  if (force_value.has_value())
  {
    return *force_value;
  } else {
    return std::make_pair(ceph::io_exerciser::Sequence::SEQUENCE_BEGIN,
                          ceph::io_exerciser::Sequence::SEQUENCE_END);
  }
}



ceph::io_sequence::tester::SelectErasureKM::SelectErasureKM(
  ceph::util::random_number_generator<int>& rng, 
  po::variables_map vm)
  : ProgramOptionSelector(rng, vm, "km", true, true)
{
}



ceph::io_sequence::tester::SelectErasurePlugin::SelectErasurePlugin(
  ceph::util::random_number_generator<int>& rng,
  po::variables_map vm)
  : ProgramOptionSelector(rng, vm, "plugin", true, false)
{
}



ceph::io_sequence::tester::SelectErasureChunkSize::SelectErasureChunkSize(ceph::util::random_number_generator<int>& rng, po::variables_map vm)
  : ProgramOptionSelector(rng, vm, "stripe_unit", true, false)
{
}



ceph::io_sequence::tester::SelectECPool::SelectECPool(
  ceph::util::random_number_generator<int>& rng,
  po::variables_map vm,
  librados::Rados& rados,
  bool dry_run)
  : ProgramOptionSelector(rng, vm, "pool", false, false),
    rados(rados),
    dry_run(dry_run),
    skm(SelectErasureKM(rng, vm)),
    spl(SelectErasurePlugin(rng, vm)),
    scs(SelectErasureChunkSize(rng, vm))
{
  if (!skm.isForced()) {
    if (vm.count("pool")) {
      force_value = vm["pool"].as<std::string>();
    }
  }
}

const std::string ceph::io_sequence::tester::SelectECPool::choose()
{
  std::pair<int,int> value;
  if (!skm.isForced() && force_value.has_value()) {
    return *force_value;
  } else {
    value = skm.choose();
  }
  int k = value.first;
  int m = value.second;
  
  const std::string plugin = std::string(spl.choose());
  const uint64_t chunk_size = scs.choose();

  std::string pool_name = "ec_" + plugin + 
                          "_cs" + std::to_string(chunk_size) +
                          "_k" + std::to_string(k) +
                          "_m" + std::to_string(m);
  if (!dry_run)
  {
    create_pool(rados, pool_name, plugin, chunk_size, k, m);
  }
  return pool_name;
}

void ceph::io_sequence::tester::SelectECPool::create_pool(
  librados::Rados& rados,
  const std::string& pool_name,
  const std::string& plugin,
  uint64_t chunk_size,
  int k, int m)
{
  int rc;
  bufferlist inbl, outbl;
  std::string profile_create =
    "{\"prefix\": \"osd erasure-code-profile set\", \
    \"name\": \"testprofile-" + pool_name + "\", \
    \"profile\": [ \"plugin=" + plugin + "\", \
    \"k=" + std::to_string(k) + "\", \
    \"m=" + std::to_string(m) + "\", \
    \"stripe_unit=" + std::to_string(chunk_size) + "\", \
    \"crush-failure-domain=osd\"]}";
  rc = rados.mon_command(profile_create, inbl, &outbl, nullptr);
  ceph_assert(rc == 0);
  std::string cmdstr =
    "{\"prefix\": \"osd pool create\", \
    \"pool\": \"" + pool_name + "\", \
    \"pool_type\": \"erasure\", \
    \"pg_num\": 8, \
    \"pgp_num\": 8, \
    \"erasure_code_profile\": \"testprofile-" + pool_name + "\"}";
  rc = rados.mon_command(cmdstr, inbl, &outbl, nullptr);
  ceph_assert(rc == 0);
}



ceph::io_sequence::tester::TestObject::TestObject( const std::string oid,
                        librados::Rados& rados,
                        boost::asio::io_context& asio,
                        SelectBlockSize& sbs,
                        SelectECPool& spo,
                        SelectObjectSize& sos,
                        SelectNumThreads& snt,
                        SelectSeqRange & ssr,
                        ceph::util::random_number_generator<int>& rng,
                        ceph::mutex& lock,
                        ceph::condition_variable& cond,
                        bool dryrun,
                        bool verbose,
                        bool has_seqseed,
                        int  seqseed) :
  rng(rng), verbose(verbose), has_seqseed(has_seqseed), seqseed(seqseed)
{
  if (dryrun) {
    verbose = true;
    exerciser_model = std::make_unique<ceph::io_exerciser::ObjectModel>(oid,
                                        sbs.choose(),
                                        rng());
  } else {
    const std::string pool = spo.choose();
    int threads = snt.choose();
    exerciser_model = std::make_unique<ceph::io_exerciser::RadosIo>(rados,
                                    asio,
                                    pool,
                                    oid,
                                    sbs.choose(),
                                    rng(),
                                    threads,
                                    lock,
                                    cond);
    dout(0) << "= " << oid << " pool=" << pool
            << " threads=" << threads
            << " blocksize=" << exerciser_model->get_block_size()
            << " =" << dendl;
  }
  obj_size_range = sos.choose();
  seq_range = ssr.choose();
  curseq = seq_range.first;
  seq = ceph::io_exerciser::IoSequence::generate_sequence(curseq,
                                                          obj_size_range,
                                                          has_seqseed ?
                                                            seqseed :
                                                            rng());
  op = seq->next();
  done = false;
  dout(0) << "== " << exerciser_model->get_oid() << " " 
          << curseq << " " 
          << seq->get_name() 
          << " ==" <<dendl;
}

bool ceph::io_sequence::tester::TestObject::readyForIo()
{
  return exerciser_model->readyForIoOp(*op);
}

bool ceph::io_sequence::tester::TestObject::next()
{
  if (!done) {
    if (verbose) {
      dout(0) << exerciser_model->get_oid() 
              << " Step " << seq->get_step() << ": "
              << op->to_string(exerciser_model->get_block_size()) << dendl;
    } else {
      dout(5) << exerciser_model->get_oid() 
              << " Step " << seq->get_step() << ": " 
              << op->to_string(exerciser_model->get_block_size()) << dendl;
    }
    exerciser_model->applyIoOp(*op);
    if (op->done()) {
      ++curseq;
      if (curseq == seq_range.second) {
        done = true;
        dout(0) << exerciser_model->get_oid()
                << " Number of IOs = " << exerciser_model->get_num_io()
                << dendl;
      } else {
        seq = ceph::io_exerciser::IoSequence::generate_sequence(curseq,
                                                                obj_size_range,
                                                                has_seqseed ?
                                                                  seqseed : 
                                                                  rng());
        dout(0) << "== " << exerciser_model->get_oid() << " " 
                << curseq << " " << seq->get_name()
                << " ==" <<dendl;
        op = seq->next();
      }
    } else {
      op = seq->next();
    }
  }
  return done;
}

bool ceph::io_sequence::tester::TestObject::finished()
{
  return done;
}

int ceph::io_sequence::tester::TestObject::get_num_io()
{
  return exerciser_model->get_num_io();
}

struct Size {};
void validate(boost::any& v, const std::vector<std::string>& values,
              Size *target_type, int) {
  po::validators::check_first_occurrence(v);
  const std::string &s = po::validators::get_single_string(values);

  std::string parse_error;
  uint64_t size = strict_iecstrtoll(s, &parse_error);
  if (!parse_error.empty()) {
    throw po::validation_error(po::validation_error::invalid_option_value);
  }
  v = boost::any(size);
}

struct Pair {};
void validate(boost::any& v, const std::vector<std::string>& values,
              Pair *target_type, int) {
  po::validators::check_first_occurrence(v);
  const std::string &s = po::validators::get_single_string(values);
  auto part = ceph::split(s).begin();
  std::string parse_error;
  int first = strict_iecstrtoll(*part++, &parse_error);
  int second = strict_iecstrtoll(*part, &parse_error);
  if (!parse_error.empty()) {
    throw po::validation_error(po::validation_error::invalid_option_value);
  }
  v = boost::any(std::pair<int,int>{first,second});
}

struct PluginString {};
void validate(boost::any& v, const std::vector<std::string>& values,
              PluginString *target_type, int) {
  po::validators::check_first_occurrence(v);
  const std::string &s = po::validators::get_single_string(values);

  const std::string_view* pluginIt = std::find(
        ceph::io_sequence::tester::pluginChoices.begin(),
        ceph::io_sequence::tester::pluginChoices.end(), 
        s
  );
  if(ceph::io_sequence::tester::pluginChoices.end() == pluginIt)
  {
    throw po::validation_error(po::validation_error::invalid_option_value);
  }

  v = boost::any(*pluginIt);
}

int parse_io_seq_options(
    po::variables_map& vm,
    const po::options_description& desc,
    int argc,
    char** argv)
{  
  std::vector<std::string> unrecognized_options;
  try {
    auto parsed = po::command_line_parser(argc, argv)
      .options(desc)
      .allow_unregistered()
      .run();
    po::store(parsed, vm);
    po::notify(vm);
    unrecognized_options = po::collect_unrecognized(parsed.options,
						    po::include_positional);

    if (!unrecognized_options.empty())
    {
      std::stringstream ss;
      ss << "Unrecognised command options supplied: ";
      while (unrecognized_options.size() > 1)
      {
        ss << unrecognized_options.back().c_str() << ", ";
        unrecognized_options.pop_back();
      }
      ss << unrecognized_options.back();
      dout(0) << ss.str() << dendl;
      return 1;
    }
  } catch(const po::error& e) {
    std::cerr << "error: " << e.what() << std::endl;
    return 1;
  }

  return 0;
}

void run_test(const std::vector<
    std::shared_ptr<ceph::io_sequence::tester::TestObject>
  >& test_objects,
  ceph::mutex& lock)
{
  // Main loop of test - while not all test objects have finished
  // check to see if any are able to start a new I/O. If all test
  // objects are waiting for I/O to complete then wait on a cond
  // that is signalled each time an I/O completes

  bool started_io = true;
  bool need_wait = true;
  while (started_io || need_wait) {
    started_io = false;
    need_wait = false;
    for (auto obj = test_objects.begin(); obj != test_objects.end(); ++obj) {
      std::shared_ptr<ceph::io_sequence::tester::TestObject> to = *obj;
      if (!to->finished()) {
	lock.lock();
	bool ready = to->readyForIo();
	lock.unlock();
	if (ready)
	{
	  to->next();
	  started_io = true;
	} else {
	  need_wait = true;
	}
      }
    }
    if (!started_io && need_wait) {
      std::unique_lock l(lock);
      // Recheck with lock incase anything has changed
      for (auto obj = test_objects.begin(); obj != test_objects.end(); ++obj) {
        std::shared_ptr<ceph::io_sequence::tester::TestObject> to = *obj;
        if (!to->finished()) {
          need_wait = !to->readyForIo();
          if (!need_wait)
          {
            break;
          }
        }
      }
      need_wait = true;
    }
  }

  int total_io = 0;
  for (auto obj = test_objects.begin(); obj != test_objects.end(); ++obj) {
    std::shared_ptr<ceph::io_sequence::tester::TestObject> to = *obj;
    total_io += to->get_num_io();
    ceph_assert(to->finished());
  }
  dout(0) << "Total number of IOs = " << total_io << dendl;
}

namespace {
  constexpr std::string_view usage[] = {
    "Basic usage:",
    "",
    "ceph_test_rados_io_sequence",
    "\t Test I/O to a single object using default settings. Good for",
    "\t testing boundary conditions",
    "",
    "ceph_test_rados_io_sequence --parallel <n>",
    "\t Run parallel test to multiple objects. First object is tested with",
    "\t default settings, other objects are tested with random settings",
    "",
    "Advanced usage:",
    "",
    "ceph_test_rados_io_sequence --blocksize <b> --km <k,m> --plugin <p>",
    "                            --objectsize <min,max> --threads <t>",
    "ceph_test_rados_io_sequence --blocksize <b> --pool <p> --object <oid>",
    "                            --objectsize <min,max> --threads <t>",
    "\tCustomize the test, if a pool is specified then it defines the",
    "\tReplica/EC configuration",
    "",
    "ceph_test_rados_io_sequence --listsequence",
    "\t Display list of supported I/O sequences",
    "",
    "ceph_test_rados_io_sequence --dryrun --sequence <n>",
    "\t Show I/O that will be generated for a sequence, validate",
    "\t seqeunce has correct I/O barriers to restrict concurrency",
    "",
    "ceph_test_rados_io_sequence --seed <seed>",
    "\t Repeat a previous test with the same random numbers (seed is",
    "\t displayed at start of test), if threads = 1 then this will produce",
    "\t the exact same sequence of I/O, if threads > 1 then I/Os are issued",
    "\t in parallel so ordering might be slightly different",
    "",
    "ceph_test_rados_io_sequence --sequence <n> --seqseed <n>",
    "\t Repeat a sequence from a previous test with the same random",
    "\t numbers (seqseed is displayed at start of sequence)",
    "",
    "ceph_test_rados_io_sequence --pool <p> --object <oid> --interactive",
    "\t Execute sequence of I/O commands from stdin. Offset and length",
    "\t are specified with unit of blocksize. Supported commands:",
    "\t\t create <len>",
    "\t\t remove",
    "\t\t read|write <off> <len>",
    "\t\t read2|write2 <off> <len> <off> <len>",
    "\t\t read3|write3 <off> <len> <off> <len> <off> <len>",
    "\t\t done"
  };
}

int main(int argc, char **argv)
{
  auto args = argv_to_vec(argc, argv);
  env_to_vec(args);
  auto cct = global_init(NULL, args, CEPH_ENTITY_TYPE_CLIENT,
			 CODE_ENVIRONMENT_UTILITY, 0);
  common_init_finish(cct.get());

  librados::Rados rados;
  boost::asio::io_context asio;
  std::thread thread;
  std::optional<boost::asio::executor_work_guard<
                  boost::asio::io_context::executor_type>> guard;
  ceph::mutex lock = ceph::make_mutex("RadosIo::lock");
  ceph::condition_variable cond;

  po::options_description desc("ceph_test_rados_io options");

  desc.add_options()
    ("help,h",
      "show help message")
    ("listsequence,l",
      "show list of sequences")
    ("dryrun,d",
      "test sequence, do not issue any I/O")
    ("verbose",
      "more verbose output during test")
    ("sequence,s", po::value<int>(),
      "test specified sequence")
    ("seed", po::value<int>(),
      "seed for whole test")
    ("seqseed", po::value<int>(),
      "seed for sequence")
    ("blocksize,b", po::value<Size>(),
      "block size (default 2048)")
    ("chunksize,c", po::value<Size>(),
      "chunk size (default 4096)")
    ("pool,p", po::value<std::string>(),
      "pool name")
    ("km", po::value<Pair>(),
      "k,m EC pool profile (default 2,2)")
    ("plugin", po::value<PluginString>(),
      "EC plugin (isa or jerasure)")
    ("objectsize", po::value<Pair>(),
      "min,max object size in blocks (default 1,32)")
    ("threads,t", po::value<int>(),
      "number of threads of I/O per object (default 1)")
    ("objects,o", po::value<int>()->default_value(1),
      "number of objects to exercise in parallel");

  po::variables_map vm;
  int rc = parse_io_seq_options(vm, desc, argc, argv);
  if (rc != 0)
  {
    return rc;
  }

  if (vm.count("help")) {
    std::cout << desc << std::endl;
    for (auto line : usage) {
      std::cout << line << std::endl;
    }
    return 0;
  }

  // Seed
  int seed = time(nullptr);
  if (vm.count("seed")) {
    seed = vm["seed"].as<int>();
  }
  dout(0) << "Test using seed " << seed << dendl;
  auto rng = ceph::util::random_number_generator<int>(seed);

  bool verbose = vm.count("verbose");
  bool dryrun = vm.count("dryrun");
  bool has_seqseed = vm.count("seqseed");
  int seqseed = 0;
  if (has_seqseed) {
    seqseed = vm["seqseed"].as<int>();
  }
  int num_objects = vm["objects"].as<int>();

  if (!dryrun) {
    rc = rados.init_with_context(g_ceph_context);
    ceph_assert(rc == 0);
    rc = rados.connect();
    ceph_assert(rc == 0);

    guard.emplace(boost::asio::make_work_guard(asio));
    thread = make_named_thread("io_thread",[&asio] { asio.run(); });
  }
  
  // Select block size
  std::unique_ptr<ceph::io_sequence::tester::SelectBlockSize> sbs
        = std::make_unique<ceph::io_sequence::tester::SelectBlockSize>(rng, vm);

  // Select pool
  std::unique_ptr<ceph::io_sequence::tester::SelectECPool> spo
        = std::make_unique<ceph::io_sequence::tester::SelectECPool>(rng, vm,
                                                                    rados,
                                                                    dryrun);

  // Select object size range
  std::unique_ptr<ceph::io_sequence::tester::SelectObjectSize> sos
        = std::make_unique<ceph::io_sequence::tester::SelectObjectSize>(rng,
                                                                        vm);

  // Select number of threads
  std::unique_ptr<ceph::io_sequence::tester::SelectNumThreads> snt =
        std::make_unique<ceph::io_sequence::tester::SelectNumThreads>(rng, vm);

  // Select range of sequences
  std::unique_ptr<ceph::io_sequence::tester::SelectSeqRange> ssr;
  try {
    ssr = std::make_unique<ceph::io_sequence::tester::SelectSeqRange>(rng, vm);
  } catch(const po::error& e) {
    return 1;
  }

  // List seqeunces
  if (vm.count("listsequence")) {
    std::pair<int,int> obj_size_range = sos->choose();
    for (ceph::io_exerciser::Sequence s
          = ceph::io_exerciser::Sequence::SEQUENCE_BEGIN; 
         s < ceph::io_exerciser::Sequence::SEQUENCE_END; ++s) {
      std::unique_ptr<ceph::io_exerciser::IoSequence> seq =
      ceph::io_exerciser::IoSequence::generate_sequence(s,
                                                        obj_size_range,
                                                        has_seqseed ?
                                                          seqseed :
                                                          rng());
      dout(0) << s << " " << seq->get_name() << dendl;
    }
    return 0;
  }

  // Create a test for each object
  std::vector<std::shared_ptr<
    ceph::io_sequence::tester::TestObject>> test_objects;
    
  for (int obj = 0; obj < num_objects; obj++) {
    test_objects.push_back(
      std::make_shared<ceph::io_sequence::tester::TestObject>(
            "test" + std::to_string(obj),
            rados, asio,
            *sbs, *spo, *sos, *snt, *ssr,
            rng, lock, cond, 
            dryrun, verbose,
            has_seqseed, seqseed
      )
    );
  }
  if (!dryrun) {
    rados.wait_for_latest_osdmap();
  }

  run_test(test_objects, lock); 

  if (!dryrun) {
    guard = std::nullopt;
    asio.stop();
    thread.join();
    rados.shutdown();
  }
  return 0;
}
