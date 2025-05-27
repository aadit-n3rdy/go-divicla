// Frame the problem as an optimization task

package main

import (
	"errors"
	"fmt"
	"math/rand"

	"github.com/MaxHalford/eaopt"
)

type Vector []float64

var eaOrc *Orchestrator

func (individual Vector) Evaluate() (float64, error) {
	// sum of commitments must be 1
	// sum of utils must be <= 1 for each device

	orc := eaOrc

	var cost = float64(0.0)

	// individual size = len(orc.Nodes) * len(orc.Computes)
	if len(individual) != len(orc.Nodes)*len(orc.Computes) {
		return 0, errors.New("individual size does not match number of nodes and computes")
	}

	// individual [i*len(orc.Computes)+j] = commitment to node i from compute j
	for i := 0; i < len(orc.NodeList); i++ {
		var commitmentSum float64 = 0.0
		for j := 0; j < len(orc.CompList); j++ {
			commitmentSum += individual[i*len(orc.Computes)+j]
		}
		cost += (commitmentSum - 1) * (commitmentSum - 1) // penalize if not equal to 1
	}

	for i, cAddr := range orc.CompList {
		comp := orc.Computes[cAddr]
		var utilSum float64 = float64(comp.Util - comp.Commitment/comp.Capacity)
		for j := 0; j < len(orc.NodeList); j++ {
			utilSum += individual[j*len(orc.Computes)+i]
		}
		if utilSum > 1.0 {
			cost += 10 * (utilSum - 1) * (utilSum - 1) // penalize if greater than 1
		}
	}

	return cost, nil
}

func (individual Vector) Mutate(rng *rand.Rand) {
	// mutate the individual by randomly shifting commitments around
	iters := (rng.Intn(len(eaOrc.NodeList) * len(eaOrc.CompList))) / 8
	// each mutation will affect upto 1/4th of the commitments

	for iter := 0; iter < iters; iter++ {
		i := rng.Intn(len(eaOrc.NodeList)) // select a random node
		// select 2 random compute nodes
		j := rng.Intn(len(eaOrc.CompList))
		k := rng.Intn(len(eaOrc.CompList))
		for j == k {
			k = rng.Intn(len(eaOrc.CompList)) // ensure j != k
		}
		// select a random amount to shift
		shift := rng.Float64() * 0.1 // shift by up to 10%
		if individual[i*len(eaOrc.Computes)+j] >= shift {
			individual[i*len(eaOrc.Computes)+j] -= shift
			individual[i*len(eaOrc.Computes)+k] += shift
		} else {
			// if not enough commitment, just set to 0
			individual[i*len(eaOrc.Computes)+j] = 0
			individual[i*len(eaOrc.Computes)+k] += individual[i*len(eaOrc.Computes)+j]
		}
	}
}

// Crossover a Vector with another Vector by applying uniform crossover.
func (X Vector) Crossover(Y eaopt.Genome, rng *rand.Rand) {
	eaopt.CrossUniformFloat64(X, Y.(Vector), rng)
}

func (X Vector) Clone() eaopt.Genome {
	var XX = make(Vector, len(X))
	copy(XX, X)
	return XX
}

func MakeVector(rng *rand.Rand) eaopt.Genome {
	// Create a new Vector with random commitments
	res := make(Vector, len(eaOrc.NodeList)*len(eaOrc.CompList))
	for i := 0; i < len(eaOrc.NodeList); i++ {
		rawVals := make([]int, len(eaOrc.CompList))
		total := 0
		for j := 0; j < len(eaOrc.CompList); j++ {
			rawVals[j] = rng.Intn(5) // random commitment between 0 and 5
			total += rawVals[j]
		}
		for j := 0; j < len(eaOrc.CompList); j++ {
			// normalize the commitments to sum to 1
			res[i*len(eaOrc.Computes)+j] = float64(rawVals[j]) / float64(total)
		}
	}
	return res
}

func RunEA() (Vector, error) {
	ga, err := eaopt.NewDefaultGAConfig().NewGA()
	if err != nil {
		fmt.Println("Error creating GA:", err)
		return nil, err
	}

	ga.EarlyStop = func(ga *eaopt.GA) bool {
		return (ga.HallOfFame.FitMin() <= 0.01) // stop if the best fitness is less than or equal to 0.01
	}

	err = ga.Minimize(MakeVector)
	if err != nil {
		return nil, err
	}

	best := ga.HallOfFame[0].Genome
	return best.(Vector), nil
}
