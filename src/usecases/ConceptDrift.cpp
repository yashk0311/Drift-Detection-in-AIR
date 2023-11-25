#include "ConceptDrift.hpp"

#include "../ConceptDrift/EventCollector_rp.hpp"
#include "../ConceptDrift/EventFilter_rp.hpp"
#include "../ConceptDrift/EventGenerator_rp.hpp"
#include "../ConceptDrift/FullAggregator_rp.hpp"


using namespace std;
/**
    * We calculate the latency as the difference between the result generation timestamp for a given `time_window` and `campaign_id`
    * pair and the event timestamp of the latest record generated that belongs to that bucket.
 **/

ConceptDrift::ConceptDrift(unsigned long throughput, string pattern) :
		Dataflow() {

	generator = new EventGeneratorRP(1, rank, worldSize, throughput);
	filter = new EventFilterRP(2, rank, worldSize, pattern);  //concept drift detector
	aggregate = new FullAggregatorRP(3, rank, worldSize);
	collector = new EventCollectorRP(4, rank, worldSize);

	addLink(generator, filter);
	addLink(filter, aggregate);
	addLink(aggregate, collector);

	generator->initialize();
	filter->initialize();
	aggregate->initialize();
	collector->initialize();
}

ConceptDrift::~ConceptDrift() {

	delete generator;
	delete filter;
	delete aggregate;
	delete collector;
}

