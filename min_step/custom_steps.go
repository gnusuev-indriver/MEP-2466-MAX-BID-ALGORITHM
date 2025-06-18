package bidsteps

import (
	"errors"

	jsoniter "github.com/json-iterator/go"
)

const (
	algorithmDefault            = ""
	algorithmBidMph             = "bid_mph"
	algorithmBidMphNoExposure   = "bid_mph_no_exposure"
	algorithmBidWithRecprice    = "with_recprice"
	algorithmBidWithoutRecprice = "without_recprice"
	algorithmFixedRange         = "fixed_range"
)

var (
	errInvalidSettings = errors.New("invalid settings: segments are empty")
)

type Params struct {
	StartPrice int64
	Recprice   int64

	// BiddingSteps is a 1D array of ascending steps in abs. or percentage
	// format depending on PercentsEnabled.
	BiddingSteps    []int64
	PercentsEnabled bool

	algorithm algorithm

	RoundValue      int64
	MaxBiddingPrice int64
	CityMaxPrice    int64

	// estimated in new-order RecpriceData
	SurgeCost  int64
	SurgeRatio float64

	// Distance in meters between A to B (departure and arrival)
	Distance int64
	// Duration in seconds between A to B (departure and arrival)
	Duration int64
	// Duration in seconds between the driver to A (departure)
	ETA int64

	// "fixed_range" algorithm extremes.
	PriceRangeMin int64
	PriceRangeMax int64
}

func (r Params) IsEmpty() bool {
	return len(r.BiddingSteps) == 0
}

type customBidSettings struct {
	// Identifier of the custom bid settings' algorithm.
	AlgorithmName string `json:"algorithm_name"`

	// Segments is an optional settings if algorithm requires segmentation.
	Segments []customBidsSegment `json:"segments"`

	// Deprecated. Flag to include recprice or not in custom bidding steps estimation.
	// Should use algorithm_name to differentiate instead of passing "without_recprice".
	WithoutRecprice bool `json:"without_recprice"`
}

// NewCustomBidStepsSettings creates Params of custom bidding steps
// depending on the provided algorithm in JSON settings.
func NewCustomBidStepsSettings(defaultParams Params, jsonData string) (Params, error) {
	algorithm, err := parseCustomBidSettings([]byte(jsonData))
	if err != nil {
		return defaultParams, err
	}

	params := defaultParams
	params.PercentsEnabled = true
	params.algorithm = algorithm

	algorithm.Modify(&params)

	return params, nil
}

// CalculateBidSteps is an external lib function to calculate the bidding steps
// prices per configured algorithm in params.
func CalculateBidSteps(params Params) Result {
	algorithm := params.algorithm
	if algorithm == nil {
		algorithm = &algDefault{}
	}

	return algorithm.CalculateBidSteps(&params)
}

func parseCustomBidSettings(jsonData []byte) (algorithm, error) {
	var base customBidSettings
	if err := jsoniter.Unmarshal(jsonData, &base); err != nil {
		return nil, err
	}

	var alg algorithm

	switch base.AlgorithmName {
	case algorithmBidMph:
		alg = &algBidMph{}
	case algorithmBidWithRecprice:
		alg = &algWithRecprice{}
	case algorithmBidWithoutRecprice:
		alg = &algWithoutRecprice{}
	case algorithmBidMphNoExposure:
		alg = &algBidMphNoExposure{}
	case algorithmFixedRange:
		alg = &algFixedRange{}
	default:
		// backward compatibility
		if len(base.Segments) == 0 {
			return nil, errInvalidSettings
		}
		if base.WithoutRecprice {
			alg = &algWithoutRecprice{}
		} else {
			alg = &algWithRecprice{}
		}
	}

	if err := jsoniter.Unmarshal(jsonData, alg); err != nil {
		return nil, err
	}

	return alg, nil
}