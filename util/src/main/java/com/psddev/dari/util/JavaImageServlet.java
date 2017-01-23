package com.psddev.dari.util;

import java.awt.image.BufferedImage;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.Arrays;
import java.util.List;
import javax.imageio.ImageIO;
import javax.servlet.ServletException;
import javax.servlet.ServletOutputStream;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.imgscalr.Scalr;

@RoutingFilter.Path(application = "_image", value = "")
public class JavaImageServlet extends HttpServlet {
    private static final List<String> BASIC_COMMANDS = Arrays.asList("circle", "grayscale", "invert", "sepia", "star", "starburst", "flipH", "flipV", "sharpen", "blur"); //Commands that don't require a value
    private static final List<String> PNG_COMMANDS = Arrays.asList("circle", "star", "starburst"); //Commands that return a PNG regardless of input
    private static final String QUALITY_OPTION = "quality";
    protected static final String SERVLET_PATH = StringUtils.ensureEnd(RoutingFilter.Static.getApplicationPath("_image"), "/");

    @Override
    public void service(HttpServletRequest request, HttpServletResponse response) throws IOException, ServletException {

        String[] urlAttributes = request.getServletPath().split("/http");
        String basePath = urlAttributes[0];
        String imageUrl = "";
        if (urlAttributes.length == 1 && request.getParameter("url") != null) {
            imageUrl = request.getParameter("url");
        } else if (urlAttributes.length == 2) {
            imageUrl = "http" + urlAttributes[1];
            if (!imageUrl.contains("://") && imageUrl.contains(":/")) {
                imageUrl = imageUrl.replace(":/", "://");
            }
        }

        if (!StringUtils.isBlank(imageUrl)) {

            String imageType = "png";
            if (imageUrl.endsWith(".jpg") || imageUrl.endsWith(".jpeg")) {
                imageType = "jpeg";
            } else if (imageUrl.endsWith(".gif")) {
                imageType = "gif";
            }

            JavaImageEditor javaImageEditor = ObjectUtils.to(JavaImageEditor.class, ImageEditor.Static.getInstance(ImageEditor.JAVA_IMAGE_EDITOR_NAME));

            String[] parameters = null;
            if (!StringUtils.isBlank(javaImageEditor.getBaseUrl())) {
                Integer parameterIndex = null;
                parameterIndex = basePath.indexOf(SERVLET_PATH) + SERVLET_PATH.length();
                parameters = basePath.substring(parameterIndex).split("/");
            } else {
                parameters = basePath.substring(SERVLET_PATH.length()).split("/");
            }

            //Verify key
            if (!StringUtils.isBlank(javaImageEditor.getSharedSecret())) {
                StringBuilder commandsBuilder = new StringBuilder();
                for (int i = 2; i < parameters.length; i++) {
                    commandsBuilder.append(StringUtils.encodeUri(parameters[i]))
                                   .append('/');
                }

                Long expireTs = (long) Integer.MAX_VALUE;
                String signature = expireTs + javaImageEditor.getSharedSecret() + StringUtils.decodeUri("/" + commandsBuilder.toString()) + imageUrl;

                String md5Hex = StringUtils.hex(StringUtils.md5(signature));
                String requestSig = md5Hex.substring(0, 7);

                if (!parameters[0].equals(requestSig) || !parameters[1].equals(expireTs.toString())) {
                    if (!StringUtils.isBlank(javaImageEditor.getErrorImage())) {
                        imageUrl = javaImageEditor.getErrorImage();
                        response.setStatus(500);
                    } else {
                        response.sendError(404);
                        return;
                    }
                }
            }

            BufferedImage bufferedImage;

            try {
                URL url = new URL(imageUrl);
                URI uri = new URI(url.getProtocol(), url.getAuthority(), url.getPath(), url.getQuery(), url.getRef());

                if ((imageUrl.endsWith("tif") || imageUrl.endsWith("tiff")) && ObjectUtils.getClassByName(JavaImageEditor.TIFF_READER_CLASS) != null) {
                    bufferedImage = JavaImageTiffReader.readTiff(uri.toString());
                } else {
                    bufferedImage = ImageIO.read(new URL(uri.toString()));
                }
            } catch (URISyntaxException ex) {
                bufferedImage = null;
            }

            if (bufferedImage == null) {
                throw new IOException(String.format("Unable to process image %s", imageUrl));
            }

            Scalr.Method quality = null;
            for (int i = 0; i < parameters.length; i = i + 2) {
                String command = parameters[i];

                if (command.equals(QUALITY_OPTION)) {
                    String value = parameters[i + 1];
                    try {
                        quality = Scalr.Method.valueOf(Scalr.Method.class, value.toUpperCase());
                    } catch (IllegalArgumentException ex) {
                        quality = javaImageEditor.findQualityByInteger(Integer.parseInt(value));
                    }
                }
            }

            for (int i = 0; i < parameters.length; i = i + 2) {
                String command = parameters[i];
                String value = i + 1 < parameters.length ? parameters[i + 1] : "";
                boolean validComand = true;

                if (command.equals(ImageEditor.RESIZE_COMMAND)) {
                    String option = null;
                    Integer width = null;
                    Integer height = null;

                    if (value.endsWith("!")) {
                        option = ImageEditor.RESIZE_OPTION_IGNORE_ASPECT_RATIO;
                    } else if (value.endsWith(">")) {
                        option = ImageEditor.RESIZE_OPTION_ONLY_SHRINK_LARGER;
                    } else if (value.endsWith("<")) {
                        option = ImageEditor.RESIZE_OPTION_ONLY_ENLARGE_SMALLER;
                    } else if (value.endsWith("^")) {
                        option = ImageEditor.RESIZE_OPTION_FILL_AREA;
                    }
                    if (option != null) {
                        value = value.substring(0, value.length() - 1);
                    }

                    String[] wh = value.split("x");
                    width = parseInteger(wh[0]);
                    if (wh.length == 2) {
                        height = parseInteger(wh[1]);
                    }

                    bufferedImage = javaImageEditor.reSize(bufferedImage, width, height, option, quality);

                } else if (command.equals(ImageEditor.CROP_COMMAND)) {
                    Integer x = 0;
                    Integer y = 0;
                    Integer width = null;
                    Integer height = null;
                    String[] size;

                    if (value.contains("+")) {
                        int delimiter = value.indexOf("+");
                        String[] xy = value.substring(delimiter + 1).split("\\+");

                        x = parseInteger(xy[0]) != null ? parseInteger(xy[0]) : 0;
                        y = parseInteger(xy[1]) != null ? parseInteger(xy[1]) : 0;

                        size = value.substring(0, delimiter).split("x");

                    } else {
                        size = value.split("x");
                        if (size.length > 3) {
                            x = parseInteger(size[0]) != null ? parseInteger(size[0]) : 0;
                            y = parseInteger(size[1]) != null ? parseInteger(size[1]) : 0;
                            size[0] = size[2];
                            size[1] = size[3];
                        }
                    }

                    width = parseInteger(size[0]);
                    if (size.length > 1) {
                        height = parseInteger(size[1]);
                    }

                    bufferedImage = javaImageEditor.crop(bufferedImage, x, y, width, height);

                } else if (command.equals(JavaImageEditor.THUMBNAIL_COMMAND)) {
                    String option = null;

                    if (value.endsWith("!")) {
                        option = ImageEditor.RESIZE_OPTION_IGNORE_ASPECT_RATIO;
                    } else if (value.endsWith(">")) {
                        option = ImageEditor.RESIZE_OPTION_ONLY_SHRINK_LARGER;
                    } else if (value.endsWith("<")) {
                        option = ImageEditor.RESIZE_OPTION_ONLY_ENLARGE_SMALLER;
                    } else if (value.endsWith("^")) {
                        option = ImageEditor.RESIZE_OPTION_FILL_AREA;
                    }
                    if (option != null) {
                        value = value.substring(0, value.length() - 1);
                    }

                    String[] wh = value.split("x");
                    if (ObjectUtils.isBlank(wh) || wh.length < 2) {
                        continue;
                    }
                    Integer width = ObjectUtils.to(Integer.class, wh[0]);
                    Integer height = ObjectUtils.to(Integer.class, wh[1]);

                    int resizeHeight =  height;
                    int resizeWidth = width;

                    if (option == null || !option.equals(ImageEditor.RESIZE_OPTION_IGNORE_ASPECT_RATIO)) {
                        resizeHeight = (int) ((double) bufferedImage.getHeight() / (double) bufferedImage.getWidth() * (double) width);
                        resizeWidth  = (int) ((double) bufferedImage.getWidth() / (double) bufferedImage.getHeight() * (double) height);
                    }

                    bufferedImage = javaImageEditor.reSize(bufferedImage, resizeWidth, resizeHeight, option, quality);
                    if ((width != bufferedImage.getWidth() || height != bufferedImage.getHeight())) {

                        //Allows for crop when reSized size is slightly off
                        if (width > bufferedImage.getWidth() && (width - 2) <= bufferedImage.getWidth()) {
                            width = bufferedImage.getWidth();
                        }

                        if (height > bufferedImage.getHeight() && (height - 2) <= bufferedImage.getHeight()) {
                            height = bufferedImage.getHeight();
                        }

                        int x = 0;
                        int y = 0;

                        //center automatic crop
                        if (bufferedImage.getWidth() > width) {
                            x = (bufferedImage.getWidth() - width) / 2;
                        }
                        if (bufferedImage.getHeight() > height) {
                            y = (bufferedImage.getHeight() - height) / 2;
                        }

                        if (width <= bufferedImage.getWidth() && height <= bufferedImage.getHeight()) {
                            bufferedImage = javaImageEditor.crop(bufferedImage, x, y, width, height);
                        }
                    }

                } else if (command.equals("grayscale")) {
                    bufferedImage = javaImageEditor.grayscale(bufferedImage);

                } else if (command.equals("brightness")) {
                    String[] wh = value.split("x");
                    Double brightness = Double.valueOf(wh[0]);
                    Double contrast = wh.length > 1 ? Double.valueOf(wh[1]) : 0.0d;

                    if (Math.abs(brightness) < 0) {
                        brightness *= 100;
                    }

                    if (Math.abs(contrast) < 0) {
                        contrast *= 100;
                    }

                    bufferedImage = javaImageEditor.brightness(bufferedImage, brightness.intValue(), contrast.intValue());

                } else if (command.equals("sharpen")) {
                    Integer ammount = null;
                    try {
                        ammount = Integer.parseInt(value);
                    } catch (NumberFormatException ex) {
                        ammount = 2;
                    }
                    bufferedImage = javaImageEditor.sharpen(bufferedImage, ammount);

                } else if (command.equals("blur")) {
                    int defaultBlur = 1;

                    if (value.contains("x")) {
                        String[] axywh = value.split("x");
                        int ammount = defaultBlur;
                        int sizeOffset = 0;
                        if (axywh.length > 4) {
                            ammount = Integer.parseInt(axywh[0]);
                            sizeOffset = 1;
                        }
                        int x = Integer.parseInt(axywh[sizeOffset]);
                        int y = Integer.parseInt(axywh[sizeOffset + 1]);
                        int w = Integer.parseInt(axywh[sizeOffset + 2]);
                        int h = Integer.parseInt(axywh[sizeOffset + 3]);

                        bufferedImage = javaImageEditor.blurArea(bufferedImage, ammount, x, y, w, h);
                    } else {
                        Integer ammount = null;
                        try {
                            ammount = Integer.parseInt(value);
                        } catch (NumberFormatException ex) {
                            ammount = defaultBlur;
                        }
                        bufferedImage = javaImageEditor.blur(bufferedImage, ammount);
                    }

                } else if (command.equals("contrast")) {
                    Double contrast = Double.valueOf(value);
                    if (Math.abs(contrast) < 0) {
                        contrast *= 100;
                    }

                    bufferedImage = javaImageEditor.brightness(bufferedImage, 0, contrast.intValue());

                } else if (command.equals("flipflop")) {
                    if (value.equals("horizontal")) {
                        bufferedImage = javaImageEditor.flipHorizontal(bufferedImage);
                    } else if (value.equals("vertical")) {
                        bufferedImage = javaImageEditor.flipVertical(bufferedImage);
                    }
                } else if (command.equals("flipH")) {
                    bufferedImage = javaImageEditor.flipHorizontal(bufferedImage);
                } else if (command.equals("flipV")) {
                    bufferedImage = javaImageEditor.flipVertical(bufferedImage);
                } else if (command.equals("invert")) {
                    bufferedImage = javaImageEditor.invert(bufferedImage);

                } else if (command.equals("rotate")) {
                    bufferedImage = javaImageEditor.rotate(bufferedImage, Integer.valueOf(parameters[i + 1]));

                } else if (command.equals("sepia")) {
                    bufferedImage = javaImageEditor.sepia(bufferedImage);

                } else if (command.equals("format")) {
                    imageType = value;

                } else if (command.equals("circle")) {
                    bufferedImage = javaImageEditor.circle(bufferedImage);

                } else if (command.equals("star")) {
                    bufferedImage = javaImageEditor.star(bufferedImage);

                } else if (command.equals("starburst")) {
                    int size = 5;
                    int count = 30;
                    if (value.contains("x")) {
                        String[] sc = value.split("x");
                        if (!StringUtils.isBlank(sc[0])) {
                            size = Integer.parseInt(sc[0]);
                        }
                        if (sc.length > 1 && !StringUtils.isBlank(sc[1])) {
                            count = Integer.parseInt(sc[1]);
                        }
                    }
                    bufferedImage = javaImageEditor.starburst(bufferedImage, size, count);

                } else {
                    validComand = false;
                }

                if (PNG_COMMANDS.contains(command)) {
                    imageType = "png";
                }

                //shift offset if a command wasn't found or a basic command has no value
                if (!validComand || (BASIC_COMMANDS.contains(command) && !StringUtils.isBlank(value) && !value.toLowerCase().equals("true"))) {
                    i = i - 1;
                }
            }

            response.setContentType("image/" + imageType);
            ServletOutputStream out = response.getOutputStream();
            ImageIO.write(bufferedImage, imageType, out);

            out.close();
        } else {
            throw new IOException("No source image provided");
        }
    }

    private Integer parseInteger(String integer) {
        if (StringUtils.isBlank(integer) || integer.matches("null")) {
            return null;
        } else {
            return Integer.parseInt(integer);
        }
    }
}
