#include "ConceptDrift.hpp"

#include "../ConceptDrift/EventCollector_cd.hpp"
#include "../ConceptDrift/CUSUM_cd.hpp"
#include "../ConceptDrift/ADWIN_cd.hpp"
#include "../ConceptDrift/EventGenerator_cd.hpp"

using namespace std;
/**
    * We calculate the latency as the difference between the result generation timestamp for a given `time_window` and `campaign_id`
    * pair and the event timestamp of the latest record generated that belongs to that bucket.
 **/

ConceptDrift::ConceptDrift(unsigned long throughput, string drift_type, unsigned long drift_rate) :
		Dataflow() {

	generator = new EventGeneratorCD(1, rank, worldSize, throughput, drift_rate);
	filter = new ADWIN_cd(2, rank, worldSize, drift_type);  //ADWIN concept drift detector
	// filter = new CUSUM_cd(2, rank, worldSize, drift_type);  //CUSUM concept drift detector
	collector = new EventCollectorCD(3, rank, worldSize);

	addLink(generator, filter);
	addLink(filter, collector);

	generator->initialize();
	filter->initialize();
	collector->initialize();
}

ConceptDrift::~ConceptDrift() {

	delete generator;
	delete filter;
	delete collector;
}