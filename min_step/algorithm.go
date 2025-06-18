package bidsteps

import (
	"slices"
)

// Custom bidding steps algorithms.
//
// https://indriver.atlassian.net/wiki/spaces/ME/pages/1864532174

const (
	algorithmBidMphDefault                = algorithmBidMph + "_default"
	algorithmBidMphRecalculated           = algorithmBidMph + "_recalculated"
	algorithmBidMphNoExposureDefault      = algorithmBidMphNoExposure + "_default"
	algorithmBidMphNoExposureRecalculated = algorithmBidMphNoExposure + "_recalculated"
)

type Result struct {
	AlgorithmName string
	BidSteps      []int64
}

type algorithm interface {
	// Modify modifies Params fields according to the algorithm.
	Modify(params *Params)

	// CalculateBidSteps calculates the final bidding steps' prices
	// in absolute format based on the provided parameters and the selected algorithm.
	// The result includes both the bidding steps and the algorithm name for analytics purposes.
	CalculateBidSteps(params *Params) Result
}

// sanitizeBidPriceSteps performs rounding, validation of each bidding price.
func sanitizeBidPriceSteps(params *Params, priceValues []int64) []int64 {
	bidPriceSteps := make([]int64, 0, len(priceValues))

	for _, priceValue := range priceValues {
		// 1 - round up
		if params.RoundValue > 0 {
			if mod := priceValue % params.RoundValue; mod != 0 {
				priceValue += params.RoundValue - mod
			}
		}

		if priceValue <= 0 {
			continue
		}

		// 2 - check have similar price before appendPrice
		lenPrices := len(bidPriceSteps)
		if lenPrices > 0 && bidPriceSteps[lenPrices-1] == priceValue {
			continue
		}

		// 3 - from bidding service (taximeter limit from bidding)
		if params.CityMaxPrice > 0 && priceValue > params.CityMaxPrice {
			bidPriceSteps = append(bidPriceSteps, params.CityMaxPrice)
			break
		}

		// 4 - check MaxBiddingPrice (geo-config limit)
		if params.MaxBiddingPrice > 0 && priceValue > params.MaxBiddingPrice {
			bidPriceSteps = append(bidPriceSteps, params.MaxBiddingPrice)
			break
		}

		bidPriceSteps = append(bidPriceSteps, priceValue)
	}

	return bidPriceSteps
}

// Algorithm: default
type algDefault struct{}

func (r *algDefault) Modify(*Params) {}

// Default bidding steps' prices calculation.
func (r *algDefault) CalculateBidSteps(params *Params) Result {
	if params == nil {
		return Result{BidSteps: []int64{0}}
	}

	bidPriceSteps := make([]int64, 0, len(params.BiddingSteps))
	for i := range params.BiddingSteps {
		var priceValue int64
		if params.PercentsEnabled {
			priceValue = int64(float64(params.StartPrice) * (1 + float64(params.BiddingSteps[i])*0.01))
		} else {
			priceValue = params.StartPrice + params.BiddingSteps[i]
		}

		bidPriceSteps = append(bidPriceSteps, priceValue)
	}

	return Result{
		AlgorithmName: algorithmDefault,
		BidSteps:      sanitizeBidPriceSteps(params, bidPriceSteps),
	}
}

// Algorithm: bid_mph aka bad bids
//
//	{
//		"algorithm_name": "bid_mph",
//		"alpha": 0,
//		"t": 0
//	}
type algBidMph struct {
	customBidSettings
	AlphaParam float64 `json:"alpha"`
	TParam     float64 `json:"t"`
}

// Modify of algBidMph does not need to modify any Params.
func (r *algBidMph) Modify(*Params) {}

// CalculateBidSteps of algBidMph requires the initial calculated bidPriceSteps,
// which is based on provided params.BiddingSteps.
// Then it finds the maximum bid to decide if bidPriceSteps should be recalculated or not.
func (r *algBidMph) CalculateBidSteps(params *Params) Result {
	result := (&algDefault{}).CalculateBidSteps(params)

	if len(result.BidSteps) == 0 || params.Duration == 0 {
		return Result{
			AlgorithmName: algorithmBidMphDefault,
			BidSteps:      result.BidSteps,
		}
	}

	maxBid := calcMaxBid(params, r.AlphaParam, r.TParam)
	if slices.Max(result.BidSteps) <= maxBid {
		// default
		return Result{
			AlgorithmName: algorithmBidMphDefault,
			BidSteps:      result.BidSteps,
		}
	}

	// recalculated
	nSteps := int64(len(result.BidSteps))
	step := (maxBid - params.StartPrice) / nSteps

	for i := range nSteps {
		priceValue := params.StartPrice + (i+1)*step
		result.BidSteps[i] = priceValue
	}

	return Result{
		AlgorithmName: algorithmBidMphRecalculated,
		BidSteps:      sanitizeBidPriceSteps(params, result.BidSteps),
	}
}

func calcMaxBid(params *Params, alphaParam, tParam float64) int64 {
	recprice := float64(params.Recprice)
	startprice := float64(params.StartPrice)
	duration := float64(params.Duration)
	eta := max(float64(params.ETA), tParam)

	maxBid := (1 + alphaParam) * max(recprice, startprice) * (duration + eta) / (duration + tParam)
	return int64(maxBid)
}

// Algorithm: with_recprice
//
//	{
//		"algorithm_name": "with_recprice",
//		"segments": [
//			{
//				"start": 0,
//				"values": [5,10,15]
//			},
//			...
//		]
//	}
type algWithRecprice struct {
	customBidSettings
}

func (r *algWithRecprice) Modify(params *Params) {
	if err := validateSegments(r.Segments); err != nil {
		return
	}

	priceForSegmentSearch := max(params.Recprice, params.StartPrice)
	foundSegment := findSegment(priceForSegmentSearch, r.Segments)
	biddingSteps := foundSegment.Values

	if params.StartPrice < params.Recprice {
		params.StartPrice = params.Recprice
		// remove last bidding segment value due to initial step
		last := len(biddingSteps) - 1
		if len(biddingSteps) == 1 {
			last = 1
		}
		biddingSteps = append([]int64{0}, biddingSteps[:last]...)
	}

	params.BiddingSteps = biddingSteps
}

func (r *algWithRecprice) CalculateBidSteps(params *Params) Result {
	result := (&algDefault{}).CalculateBidSteps(params)
	result.AlgorithmName = algorithmBidWithRecprice

	return result
}

// Algorithm: without_recprice
//
//	{
//		"algorithm_name": "without_recprice",
//		"segments": [
//			{
//				"start": 0,
//				"values": [5,10,15]
//			},
//			...
//		],
//		"without_recprice": true
//	}
type algWithoutRecprice struct {
	customBidSettings
}

func (r *algWithoutRecprice) Modify(params *Params) {
	if err := validateSegments(r.Segments); err != nil {
		return
	}
	segment := findSegment(params.StartPrice, r.Segments)
	params.BiddingSteps = segment.Values
}

func (r *algWithoutRecprice) CalculateBidSteps(params *Params) Result {
	result := (&algDefault{}).CalculateBidSteps(params)
	result.AlgorithmName = algorithmBidWithoutRecprice

	return result
}

// Algorithm: bid_mph_no_exposure aka bad bids "No Exposure"
//
//	{
//		"algorithm_name": "bid_mph_no_exposure",
//		"alpha": 0,
//		"t": 0
//	}
type algBidMphNoExposure struct {
	customBidSettings
	AlphaParam float64 `json:"alpha"`
	TParam     float64 `json:"t"`
}

func (r *algBidMphNoExposure) Modify(*Params) {}

func (r *algBidMphNoExposure) CalculateBidSteps(params *Params) Result {
	result := (&algDefault{}).CalculateBidSteps(params)

	if len(result.BidSteps) == 0 || params.Duration == 0 {
		return Result{
			AlgorithmName: algorithmBidMphNoExposureDefault,
			BidSteps:      result.BidSteps,
		}
	}

	maxBid := calcMaxBid(params, r.AlphaParam, r.TParam)

	algorithmName := algorithmBidMphNoExposureDefault
	for i, priceValue := range result.BidSteps {
		if priceValue > maxBid {
			result.BidSteps[i] = maxBid
			algorithmName = algorithmBidMphNoExposureRecalculated
		}
	}

	return Result{
		AlgorithmName: algorithmName,
		BidSteps:      sanitizeBidPriceSteps(params, result.BidSteps),
	}
}

// Algorithm: fixed_range
//
//	{
//		"algorithm_name": "fixed_range",
//		"ratio": 0.98
//	}
type algFixedRange struct {
	customBidSettings
	Ratio float64 `json:"ratio"`
}

// Modify calculates PriceRangeMin, PriceRangeMax via Recprice and given Ratio.
func (r *algFixedRange) Modify(params *Params) {
	params.PriceRangeMin = int64(float64(params.Recprice) * r.Ratio)
	params.PriceRangeMax = int64(float64(params.Recprice) * (1 + (1 - r.Ratio)))
}

func (r *algFixedRange) CalculateBidSteps(params *Params) Result {
	bidSteps := []int64{
		params.PriceRangeMin + (params.PriceRangeMax-params.PriceRangeMin)/3,
		params.PriceRangeMin + (params.PriceRangeMax-params.PriceRangeMin)/3*2,
		params.PriceRangeMax,
	}
	return Result{
		AlgorithmName: algorithmFixedRange,
		BidSteps:      sanitizeBidPriceSteps(params, bidSteps),
	}
}