package poly.dto;

public class QuizRankStatDTO {

	private String stdDay; // 기준일자
	private String qp_id; // 퀴즈백 아이디

	private String qp_rank; // 인기순위
	private String qp_nm; // 기준일자
	private String qp_tag; // 퀴즈백 아이디

	// 조회수 분석
	private double readRate;
	private double readRateWeight;

	// 좋아요 분석
	private double likeRate;
	private double likeRateWeight;

	// 찜 분석
	private double zzimRate;
	private double zzimRateWeight;

	// 공유하기 분석
	private double shareRate;
	private double shareRateWeight;

	// 정답률 분석
	private double trueRate;
	private double trueRateWeight;

	// 진행률 분석
	private double prgRate;
	private double prgRateWeight;

	// 조회수 증가율 분석
	private double readIcrRate;
	private double readIcrRateWeight;

	// 최종 결과
	private double rateWeight;

	public String getStdDay() {
		return stdDay;
	}

	public void setStdDay(String stdDay) {
		this.stdDay = stdDay;
	}

	public String getQp_id() {
		return qp_id;
	}

	public void setQp_id(String qp_id) {
		this.qp_id = qp_id;
	}

	public String getQp_rank() {
		return qp_rank;
	}

	public void setQp_rank(String qp_rank) {
		this.qp_rank = qp_rank;
	}

	public String getQp_nm() {
		return qp_nm;
	}

	public void setQp_nm(String qp_nm) {
		this.qp_nm = qp_nm;
	}

	public String getQp_tag() {
		return qp_tag;
	}

	public void setQp_tag(String qp_tag) {
		this.qp_tag = qp_tag;
	}

	public double getReadRate() {
		return readRate;
	}

	public void setReadRate(double readRate) {
		this.readRate = readRate;
	}

	public double getReadRateWeight() {
		return readRateWeight;
	}

	public void setReadRateWeight(double readRateWeight) {
		this.readRateWeight = readRateWeight;
	}

	public double getLikeRate() {
		return likeRate;
	}

	public void setLikeRate(double likeRate) {
		this.likeRate = likeRate;
	}

	public double getLikeRateWeight() {
		return likeRateWeight;
	}

	public void setLikeRateWeight(double likeRateWeight) {
		this.likeRateWeight = likeRateWeight;
	}

	public double getZzimRate() {
		return zzimRate;
	}

	public void setZzimRate(double zzimRate) {
		this.zzimRate = zzimRate;
	}

	public double getZzimRateWeight() {
		return zzimRateWeight;
	}

	public void setZzimRateWeight(double zzimRateWeight) {
		this.zzimRateWeight = zzimRateWeight;
	}

	public double getShareRate() {
		return shareRate;
	}

	public void setShareRate(double shareRate) {
		this.shareRate = shareRate;
	}

	public double getShareRateWeight() {
		return shareRateWeight;
	}

	public void setShareRateWeight(double shareRateWeight) {
		this.shareRateWeight = shareRateWeight;
	}

	public double getTrueRate() {
		return trueRate;
	}

	public void setTrueRate(double trueRate) {
		this.trueRate = trueRate;
	}

	public double getTrueRateWeight() {
		return trueRateWeight;
	}

	public void setTrueRateWeight(double trueRateWeight) {
		this.trueRateWeight = trueRateWeight;
	}

	public double getPrgRate() {
		return prgRate;
	}

	public void setPrgRate(double prgRate) {
		this.prgRate = prgRate;
	}

	public double getPrgRateWeight() {
		return prgRateWeight;
	}

	public void setPrgRateWeight(double prgRateWeight) {
		this.prgRateWeight = prgRateWeight;
	}

	public double getReadIcrRate() {
		return readIcrRate;
	}

	public void setReadIcrRate(double readIcrRate) {
		this.readIcrRate = readIcrRate;
	}

	public double getReadIcrRateWeight() {
		return readIcrRateWeight;
	}

	public void setReadIcrRateWeight(double readIcrRateWeight) {
		this.readIcrRateWeight = readIcrRateWeight;
	}

	public double getRateWeight() {
		return rateWeight;
	}

	public void setRateWeight(double rateWeight) {
		this.rateWeight = rateWeight;
	}

}
