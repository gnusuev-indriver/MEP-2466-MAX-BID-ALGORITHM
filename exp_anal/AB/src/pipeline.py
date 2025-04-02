import numpy as np
from scipy.stats import ttest_ind
from statsmodels.stats.power import TTestIndPower


class RatioMetricHypothesisTestingPipeline:
    def __init__(self, data, metric, numerator, denominator, groups):
        self.data = data[["group_name", numerator, denominator]].fillna(0)
        self.metric = metric
        self.numerator = numerator
        self.denominator = denominator
        self.control = groups["control"]
        self.treatment = groups["treatment"]
        self.result: dict = {}
        self.skip = False

    def run(self):
        self.check_zero_denominator()
        if self.skip is False:
            self.calc_values()
            self.linearize_data()
            self.calc_pvalue()
            self.calc_effect_size()
            self.calc_n_obs()
            self.calc_power()
            self.calc_obs_needed()

    def check_zero_denominator(self):
        df = self.data
        if (
            df[df["group_name"] == self.control][self.denominator].sum() == 0
            or df[df["group_name"] == self.treatment][self.denominator].sum() == 0
        ):
            self.result["metric"] = self.metric
            self.result["control_value"] = None
            self.result["experimental_value"] = None
            self.result["uplift_abs"] = None
            self.result["uplift_rel"] = None
            self.result["pvalue"] = None
            self.result["effect_size"] = None
            self.calc_n_obs()
            self.result["power"] = None
            self.result["obs_needed"] = None
            self.skip = True

    def calc_values(self):
        df = self.data
        self.result["metric"] = self.metric
        self.result["control_value"] = (
            df[df["group_name"] == self.control][self.numerator].sum()
            / df[df["group_name"] == self.control][self.denominator].sum()
        )
        self.result["experimental_value"] = (
            df[df["group_name"] == self.treatment][self.numerator].sum()
            / df[df["group_name"] == self.treatment][self.denominator].sum()
        )
        self.result["uplift_abs"] = (
            self.result["experimental_value"] - self.result["control_value"]
        )
        self.result["uplift_rel"] = (
            self.result["uplift_abs"] / self.result["control_value"]
        )

    def linearize_data(self):
        df = self.data
        conversions = df[df["group_name"] == self.control][self.numerator].sum()
        visitors = df[df["group_name"] == self.control][self.denominator].sum()
        k = conversions / visitors
        df[f"linearized_{self.denominator}"] = k * df[self.denominator]
        df[f"linearized_{self.numerator}"] = (
            df[self.numerator] - df[f"linearized_{self.denominator}"]
        ).astype(float)

    def calc_pvalue(self):
        """T-test for the means of two independent samples"""
        df = self.data
        control_lin = df[df["group_name"] == self.control][
            f"linearized_{self.numerator}"
        ]
        experimental_lin = df[df["group_name"] == self.treatment][
            f"linearized_{self.numerator}"
        ]
        t, self.result["pvalue"] = ttest_ind(
            control_lin, experimental_lin, random_state=42
        )

    def calc_effect_size(self):
        """Cohen's d"""
        df = self.data
        control_lin = df[df["group_name"] == self.control][
            f"linearized_{self.numerator}"
        ]
        experimental_lin = df[df["group_name"] == self.treatment][
            f"linearized_{self.numerator}"
        ]
        n1, n2 = len(control_lin), len(experimental_lin)
        s1, s2 = np.var(control_lin, ddof=1), np.var(experimental_lin, ddof=1)
        s = np.sqrt(((n1 - 1) * s1 + (n2 - 1) * s2) / (n1 + n2 - 2))
        u1, u2 = np.mean(control_lin), np.mean(experimental_lin)
        self.result["effect_size"] = (u2 - u1) / s

    def calc_n_obs(self):
        self.result["n_obs_control"] = self.data["group_name"].value_counts()[
            self.control
        ]
        self.result["n_obs_experimental"] = self.data["group_name"].value_counts()[
            self.treatment
        ]

    def calc_power(self):
        power_analysis = TTestIndPower()
        self.result["power"] = power_analysis.solve_power(
            effect_size=self.result["effect_size"],
            nobs1=self.result["n_obs_control"],
            alpha=0.05,
            ratio=self.result["n_obs_experimental"] / self.result["n_obs_control"],
        )

    def calc_obs_needed(self):
        power_analysis = TTestIndPower()
        self.result["obs_needed"] = 2 * power_analysis.solve_power(
            effect_size=self.result["effect_size"], power=0.8, alpha=0.05
        )