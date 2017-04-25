package com.mr.udacity.patterns;

import java.util.Comparator;

public class Blog {

	int sizeOfBlog;
	String blogContent;

	public Blog(String blog){
		this.blogContent = blog;
	}
	
	public int getSizeOfBlog() {
		return blogContent.length();
	}

	public void setSizeOfBlog(int sizeOfBlog) {
		this.sizeOfBlog = blogContent.length();
	}

	public String getBlogContent() {
		return blogContent;
	}

	public void setBlogContent(String blogContent) {
		this.blogContent = blogContent;
	}
}

class BlogSizeCompare implements Comparator<Blog>{

	@Override
	public int compare(Blog o1, Blog o2) {
		if(o1.getSizeOfBlog()<o2.getSizeOfBlog()){
			return 1;
		}
		else
		{
			return -1;
		}
	}
	
}