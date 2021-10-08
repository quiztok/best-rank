package poly.dto.redis;

public class RankResultDTO {

	private String qp_id;
	private String qp_nm;
	private int qp_rank;
	private double rate_weight;
	private String error;

	public String getQp_id() {
		return qp_id;
	}

	public void setQp_id(String qp_id) {
		this.qp_id = qp_id;
	}

	public String getQp_nm() {
		return qp_nm;
	}

	public void setQp_nm(String qp_nm) {
		this.qp_nm = qp_nm;
	}

	public int getQp_rank() {
		return qp_rank;
	}

	public void setQp_rank(int qp_rank) {
		this.qp_rank = qp_rank;
	}

	public double getRate_weight() {
		return rate_weight;
	}

	public void setRate_weight(double rate_weight) {
		this.rate_weight = rate_weight;
	}

	public String getError() {
		return error;
	}

	public void setError(String error) {
		this.error = error;
	}

}
