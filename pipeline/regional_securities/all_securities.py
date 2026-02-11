from .us_securities import US_SECURITIES
from .eu_securities import EU_SECURITIES
from .ch_securities import CH_SECURITIES

ALL_SECURITIES = sorted(
    set(
        US_SECURITIES
        + EU_SECURITIES
        + CH_SECURITIES
    )
)