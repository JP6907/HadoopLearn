package mapreduce.page.topn;

public class PageCount implements Comparable<PageCount>{

    private String page;
    private int count;

    public PageCount() {
    }

    public PageCount(String page, int count) {
        this.page = page;
        this.count = count;
    }

    public String getPage() {
        return page;
    }

    public void setPage(String page) {
        this.page = page;
    }

    public int getCount() {
        return count;
    }

    public void setCount(int count) {
        this.count = count;
    }

    public int compareTo(PageCount o) {
        return o.count-this.count==0?o.page.compareTo(this.page):o.count-this.count;
    }
}
