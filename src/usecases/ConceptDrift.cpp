#include "ConceptDrift.hpp"

#include "../ConceptDrift/EventCollector_cd.hpp"
#include "../ConceptDrift/EventFilter_cd.hpp"
#include "../ConceptDrift/EventGenerator_cd.hpp"
#include "../ConceptDrift/FullAggregator_cd.hpp"


using namespace std;
/**
    * We calculate the latency as the difference between the result generation timestamp for a given `time_window` and `campaign_id`
    * pair and the event timestamp of the latest record generated that belongs to that bucket.
 **/

ConceptDrift::ConceptDrift(unsigned long throughput, string drift_type, unsigned long drift_rate) :
		Dataflow() {

	generator = new EventGeneratorCD(1, rank, worldSize, throughput, drift_rate);
	filter = new EventFilterCD(2, rank, worldSize, drift_type);  //concept drift detector
	// aggregate = new FullAggregatorCD(3, rank, worldSize);
	collector = new EventCollectorCD(3, rank, worldSize);

	addLink(generator, filter);
	// addLink(filter, aggregate);
	addLink(filter, collector);

	generator->initialize();
	filter->initialize();
	// aggregate->initialize();
	collector->initialize();
}

ConceptDrift::~ConceptDrift() {

	delete generator;
	delete filter;
	// delete aggregate;
	delete collector;
}