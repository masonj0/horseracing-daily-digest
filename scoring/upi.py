from typing import Optional, List
from models import RunnerFeatures, RunnerForm


def _nz(x: Optional[float], default: float = 0.0) -> float:
    return float(x) if isinstance(x, (int, float)) else default


def _score_form(forms: List[RunnerForm]) -> float:
    """Recency-weighted form: lower pos and btn better. Returns 0..100."""
    if not forms:
        return 50.0
    score = 0.0
    weight = 1.0
    total_w = 0.0
    for f in forms[:5]:  # last 5
        pos = _nz(f.pos, 6.0)
        btn = _nz(f.btn, 5.0)
        s = max(0.0, 100.0 - (pos - 1) * 12.0 - btn * 6.0)
        score += s * weight
        total_w += weight
        weight *= 0.8
    return score / total_w if total_w > 0 else 50.0


def _score_jt(j: Optional[float], t: Optional[float], jt: Optional[float]) -> float:
    jw = _nz(j, 0.08)
    tw = _nz(t, 0.12)
    jtw = _nz(jt, (jw + tw) / 2)
    # cap and scale to 0..100
    return max(0.0, min(100.0, (jw * 100 * 0.4) + (tw * 100 * 0.3) + (jtw * 100 * 0.3)))


def _score_rating(r: Optional[float]) -> float:
    if r is None:
        return 50.0
    # Assume rating roughly 0..120; scale
    return max(0.0, min(100.0, (r / 120.0) * 100.0))


def _score_profile(at_course: Optional[float], at_distance: Optional[float], going_profile: Optional[float]) -> float:
    ac = _nz(at_course, 0.5)
    ad = _nz(at_distance, 0.5)
    gp = _nz(going_profile, 0.5)
    return max(0.0, min(100.0, (ac * 100 * 0.34) + (ad * 100 * 0.33) + (gp * 100 * 0.33)))


def upi_score(feat: RunnerFeatures) -> float:
    """Compute UPI v1 using simple weighted blend of components."""
    w = dict(form=0.35, jt=0.20, rating=0.25, profile=0.20)
    s_form = _score_form(feat.form)
    s_jt = _score_jt(feat.j_win, feat.t_win, feat.jt_win)
    s_rt = _score_rating(feat.rating)
    s_pf = _score_profile(feat.at_course, feat.at_distance, feat.going_profile)
    score = w['form'] * s_form + w['jt'] * s_jt + w['rating'] * s_rt + w['profile'] * s_pf
    return max(0.0, min(100.0, score))