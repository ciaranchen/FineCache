import setuptools

with open("README.md", "r", encoding='utf-8') as fh:
  long_description = fh.read()

setuptools.setup(
  name="FineCache",
  version="0.2.0",
  author="Ciaran Chen",
  author_email="ciaranchen@qq.com",
  description="科研项目中实验记录工具 ",
  long_description=long_description,
  long_description_content_type="text/markdown",
  url="https://github.com/ciaranchen/FineCache",
  packages=setuptools.find_packages(),
  classifiers=[
  "Programming Language :: Python :: 3",
  "License :: OSI Approved :: MIT License",
  "Operating System :: OS Independent",
  ],
)