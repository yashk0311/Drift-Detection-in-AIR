#ifndef USECASES_CD_HPP_
#define USECASES_CD_HPP_

#include "../dataflow/Dataflow.hpp"

using namespace std;

class ConceptDrift: public Dataflow {

public:

	Vertex *generator, *filter, *aggregate,*join,
			*collector;

	ConceptDrift(unsigned long tp, string pattern);

	~ConceptDrift();

};

#endif /* USECASES_YSB_HPP_ */
